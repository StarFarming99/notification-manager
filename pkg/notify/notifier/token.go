package notifier

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type token struct {
	accessToken   string
	accessTokenAt time.Time
	expires       time.Duration
	mutex         sync.Mutex
}

type AccessTokenService struct {
	mutex    sync.Mutex
	tokens   map[string]*token
	inFlight map[string]chan struct{}
}

var ats *AccessTokenService

func init() {
	ats = &AccessTokenService{
		tokens: make(map[string]*token),
	}
}

func GetAccessTokenService() *AccessTokenService {
	return ats
}

// Token cache locks protect only cache access. Remote token acquisition uses the
// caller deadline directly and leaves no detached goroutine after cancellation.
func (ats *AccessTokenService) InvalidToken(ctx context.Context, key string, l log.Logger) {
	if ctx.Err() != nil {
		_ = level.Error(l).Log("msg", "invalid token timeout")
		return
	}
	ats.mutex.Lock()
	defer ats.mutex.Unlock()
	if t, ok := ats.tokens[key]; ok {
		t.accessTokenAt = time.Time{}
	}
}

func (ats *AccessTokenService) GetToken(ctx context.Context, key string, getToken func(context.Context) (string, time.Duration, error)) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		ats.mutex.Lock()
		if t, ok := ats.tokens[key]; ok && time.Since(t.accessTokenAt) < t.expires {
			value := t.accessToken
			ats.mutex.Unlock()
			return value, nil
		}
		if waiting := ats.inFlight[key]; waiting != nil {
			ats.mutex.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-waiting:
				continue
			}
		}
		if ats.inFlight == nil {
			ats.inFlight = make(map[string]chan struct{})
		}
		done := make(chan struct{})
		ats.inFlight[key] = done
		ats.mutex.Unlock()

		// Only the acquiring caller performs I/O. Other callers wait with their own
		// deadlines; no token-fetch goroutine can outlive a cancelled acquisition.
		value, expires, err := getToken(ctx)
		if err == nil {
			err = ctx.Err()
		}
		ats.mutex.Lock()
		if err == nil {
			ats.tokens[key] = &token{accessToken: value, accessTokenAt: time.Now(), expires: expires}
		}
		delete(ats.inFlight, key)
		close(done)
		ats.mutex.Unlock()
		if err != nil {
			return "", err
		}
		return value, nil
	}
}
