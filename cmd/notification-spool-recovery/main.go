// notification-spool-recovery works with the PVC after the NM writer has stopped.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/utils"
	"gopkg.in/alecthomas/kingpin.v2"
)

func run() error { return runArgs(os.Args[1:]) }
func runArgs(args []string) error {
	app := kingpin.New("notification-spool-recovery", "Offline fenced original notification ledger maintenance")
	path := app.Flag("spool", "Absolute original-notification spool file").Required().String()
	action := app.Flag("action", "check, export, resolve, replay-target, seed-am-group, pause-profile").Default("check").Enum("check", "export", "resolve", "replay-target", "seed-am-group", "pause-profile")
	target := app.Flag("target", "Exact target ID; no whole-intake replay").String()
	state := app.Flag("state", "Reconciliation result").Default("delivered").Enum("delivered", "retryable", "dead_letter")
	actor := app.Flag("actor", "Audited operator identifier").String()
	evidence := app.Flag("evidence", "Receipt/change reference, never credentials").String()
	output := app.Flag("output", "New private JSON export file; must not exist").String()
	permit := app.Flag("permit-platform-send", "Explicitly permit one frozen platform request").Bool()
	profileID := app.Flag("profile", "Exact paused profile for AM index migration").String()
	profileVersion := app.Flag("profile-version", "Registered immutable profile version").String()
	expectedRevision := app.Flag("expected-profile-revision", "Paused profile CAS revision").Uint64()
	groupHash := app.Flag("am-group-hash", "Verified canonical AM receiver/group SHA256").String()
	requestHash := app.Flag("am-request-hash", "Exact canonical AM intent SHA256").String()
	intakeID := app.Flag("intake", "Proven last accepted legacy intake for this group").String()
	if _, err := app.Parse(args); err != nil {
		return err
	}
	if *action == "check" || *action == "export" {
		s, err := spool.OpenReadOnly(*path)
		if err != nil {
			return err
		}
		defer s.Close()
		if err := s.Check(); err != nil {
			return err
		}
		if *action == "check" {
			status, err := s.Status()
			if err != nil {
				return err
			}
			profiles, err := s.ProfileStatus()
			if err != nil {
				return err
			}
			status["profiles"] = profiles
			return json.NewEncoder(os.Stdout).Encode(status)
		}
		if *output == "" {
			return errors.New("export requires a private output file")
		}
		intakes, targets, err := s.Snapshot()
		if err != nil {
			return err
		}
		file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := json.NewEncoder(file).Encode(map[string]interface{}{"version": spool.Version, "intakes": intakes, "targets": targets}); err != nil {
			return err
		}
		return file.Sync()
	}
	if (*action != "seed-am-group" && *action != "pause-profile" && *target == "") || *actor == "" || *evidence == "" {
		return errors.New("mutations require exact target, actor and evidence")
	}
	if *action == "replay-target" && !*permit {
		return errors.New("replay requires explicit permit-platform-send")
	}
	s, err := spool.Open(*path, spool.Limits{})
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.Check(); err != nil {
		return err
	}
	if *action == "pause-profile" {
		if *profileID != "formal" || *expectedRevision == 0 {
			return errors.New("offline pause requires formal profile and exact persisted revision")
		}
		paused, err := s.ControlProfile("formal", "pause", "", *expectedRevision, *actor, *evidence)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"action": "pause-profile", "profile_id": "formal", "state": paused, "writer_stopped": true})
	}
	if *action == "seed-am-group" {
		return s.SeedAMGroup(*profileID, *profileVersion, *groupHash, *intakeID, *requestHash, *expectedRevision, *actor, *evidence)
	}
	if *action == "resolve" {
		return s.Resolve(*target, *state, *actor, *evidence)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logger := log.NewLogfmtLogger(os.Stderr)
	ctl, err := controller.New(ctx, logger)
	if err != nil {
		return err
	}
	if err := ctl.Run(); err != nil {
		return err
	}
	profiles, err := deliveryprofiles.FromEnv(s)
	if err != nil {
		return err
	}
	ctl.DeliveryProfiles = profiles
	claim, err := s.ClaimForRecovery(*target, *actor, *evidence, time.Now())
	if err != nil {
		return err
	}
	next := time.Now().Add(5 * time.Second)
	interval := time.Duration(0)
	if profiles != nil {
		interval = profiles.IntervalFor(claim.Target.ProfileID)
	}
	started := false
	if claim.Target.ProfileID != "" && profiles == nil {
		err = &utils.PreSendError{Err: errors.New("profiled recovery requires original delivery profile configuration and send budget")}
	} else {
		err = s.WithSendPermit(claim, interval, time.Now().UTC(), func() error {
			started = true
			return notify.SendFrozen(ctx, logger, ctl, claim.Target)
		})
		var deferred *spool.SendBudgetDeferred
		if errors.As(err, &deferred) {
			next = deferred.AvailableAt
		}
		if err != nil && !started {
			err = &utils.PreSendError{Err: err}
		}
	}
	completion, outcome := utils.DeliveryOutcome(err)
	if finishErr := s.Finish(claim, completion, outcome, "recovery-attempt:"+claim.AttemptID, next); finishErr != nil {
		return finishErr
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"target_id": claim.Target.ID, "attempt_id": claim.AttemptID, "state": completion})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
