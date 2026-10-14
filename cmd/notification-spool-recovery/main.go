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

func run() error {
	path := kingpin.Flag("spool", "Absolute original-notification spool file").Required().String()
	action := kingpin.Flag("action", "check, export, resolve, replay-target, seed-am-group").Default("check").Enum("check", "export", "resolve", "replay-target", "seed-am-group")
	target := kingpin.Flag("target", "Exact target ID; no whole-intake replay").String()
	state := kingpin.Flag("state", "Reconciliation result").Default("delivered").Enum("delivered", "retryable", "dead_letter")
	actor := kingpin.Flag("actor", "Audited operator identifier").String()
	evidence := kingpin.Flag("evidence", "Receipt/change reference, never credentials").String()
	output := kingpin.Flag("output", "New private JSON export file; must not exist").String()
	permit := kingpin.Flag("permit-platform-send", "Explicitly permit one frozen platform request").Bool()
	profileID := kingpin.Flag("profile", "Exact paused profile for AM index migration").String()
	profileVersion := kingpin.Flag("profile-version", "Registered immutable profile version").String()
	expectedRevision := kingpin.Flag("expected-profile-revision", "Paused profile CAS revision").Uint64()
	groupHash := kingpin.Flag("am-group-hash", "Verified canonical AM receiver/group SHA256").String()
	requestHash := kingpin.Flag("am-request-hash", "Exact canonical AM intent SHA256").String()
	intakeID := kingpin.Flag("intake", "Proven last accepted legacy intake for this group").String()
	kingpin.Parse()
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
	if (*action != "seed-am-group" && *target == "") || *actor == "" || *evidence == "" {
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
		interval = profiles.SendInterval
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
