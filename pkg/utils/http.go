package utils

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

func UrlWithPath(u, path string) (string, error) {

	postMessageURL, err := url.Parse(u)
	if err != nil {
		return "", err
	}

	postMessageURL.Path += path
	return postMessageURL.String(), nil
}

func UrlWithParameters(u string, parameters map[string]string) (string, error) {

	postMessageURL, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	postMessageURL.Query()
	values := postMessageURL.Query()
	for k, v := range parameters {
		values.Set(k, v)
	}

	postMessageURL.RawQuery = values.Encode()
	return postMessageURL.String(), nil
}

func DoHttpRequest(ctx context.Context, client *http.Client, request *http.Request) ([]byte, error) {

	if client == nil {
		client = &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	}

	markDelivery(ctx)
	resp, err := client.Do(request.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	body, err := ioutil.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if len(body) > 2<<20 {
		return nil, fmt.Errorf("notification response exceeds 2 MiB")
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return body, &HTTPStatusError{Status: resp.StatusCode}
	}

	return body, nil
}

// HTTPStatusError avoids leaking webhook response bodies into logs.
type HTTPStatusError struct{ Status int }

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("notification platform HTTP %d", e.Status)
}
