#!/usr/bin/env python3
"""Actual c62 stop/start with preserved local Bolt, then new-to-new handoff.

Docker --internal, synthetic Kubernetes/platform APIs and independent local
state directories only. Does not invoke kubectl/JMS or a real messaging API.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

from bluegreen_isolated import Fixture, OLD, READ, TEST


class RestartFixture(Fixture):
    def __init__(self, args):
        super().__init__(args)
        self.initial_preparation = True

    def start(self, alias, old=False, mode='active', profiles=True):
        if self.initial_preparation and alias in ('generation-a', 'generation-b'):
            return
        return super().start(alias, old, mode, profiles)

    def docker(self, *args, check=True):
        args = list(args)
        if args and args[0] == 'run' and '--entrypoint' not in args and OLD in args:
            i = args.index(OLD)
            args[i:i] = ['-e', 'NM_NOTIFICATION_SEND_INTERVAL=30s']
        return super().docker(*args, check=check)

    def export_offline(self, alias, name):
        output = self.root / name
        command = [str(self.args.recovery.resolve()), '--spool', str(self.root / alias / 'spool.db'),
                   '--action', 'export', '--output', str(output)]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode:
            raise AssertionError('offline read-only export failed: ' + result.stderr)
        return json.loads(output.read_text())

    @staticmethod
    def target_for(export, word):
        return next(row for row in export['targets'] if word in json.dumps(row['payload']))

    def execute(self):
        self.prepare()
        self.initial_preparation = False
        old, peer = 'legacy-c62', 'generation-b'
        assert self.request(old+':19094', '/internal/instance', token=READ)[0] == 404
        self.request('mock:8080', '/control/mode', {old: 'response_lost'})
        self.send(old, 'ordinary-upgrade-unknown', remember=False)
        self.wait(lambda: self.request(old+':19094', '/status')[1].get('counts', {}).get('unknown') == 1,
                  label='c62 real persisted unknown')
        self.request('mock:8080', '/control/mode', {old: 'healthy'})
        self.send(old, 'ordinary-upgrade-pending', remember=False)
        self.wait(lambda: (lambda counts: counts.get('pending', 0)+counts.get('retryable', 0) == 1)(
            self.request(old+':19094', '/status')[1]['counts']), timeout=3, label='pending pre-send budget deferral')
        assert len(self.cards()) == 1, 'pending was not held by the old send budget'
        self.docker('stop', '--time', '10', self.prefix+'-'+old)
        (self.root/'c62.log').write_text(self.docker('logs', self.prefix+'-'+old).stdout +
                                       self.docker('logs', self.prefix+'-'+old).stderr)
        before = self.export_offline(old, 'before-c62-stop.json')
        unknown_before = self.target_for(before, 'ordinary-upgrade-unknown')
        pending_before = self.target_for(before, 'ordinary-upgrade-pending')
        assert unknown_before['state'] == 'unknown' and unknown_before['attempts'] == 1, unknown_before
        assert pending_before['state'] in ('pending', 'retryable'), pending_before
        assert not any(row.get('generation') or row.get('reception_sequence') for row in before['intakes']), before['intakes']
        self.docker('rm', self.prefix+'-'+old)
        self.names.remove(self.prefix+'-'+old)
        before_bytes = (self.root/old/'spool.db').stat().st_size
        self.start(old)
        self.wait(lambda: len(self.cards()) >= 2, timeout=35, label='old pending delivered after plain restart')
        assert len(self.cards()) == 2, self.cards()
        status_after = self.request(old+':19094', '/internal/instance', token=READ)[1]
        assert status_after['spool']['counts'].get('unknown') == 1, status_after
        self.event('ordinary_c62_restart', status='PASS', short_interruption_allowed=True,
                   preserved_path=str(self.root/old/'spool.db'), before_file_bytes=before_bytes,
                   original_c62_image=OLD, online_bootstrap_required=False,
                   unknown_platform_attempts=1, pending_delivered=1, test_destination_unchanged=True)
        # New supported protocol -> another supported protocol may proceed while
        # the preserved old unknown remains unresolved and is never resent.
        self.start(peer, mode='standby')
        self.command(old, 'begin-active', peer)
        self.command(peer, 'begin-standby', old)
        self.transfer(old, peer)
        self.send(peer, 'ordinary-upgrade-next-generation', remember=False)
        self.handoff(old, peer)
        self.wait(lambda: len(self.cards()) >= 3, timeout=20, label='new generation not blocked by legacy unknown')
        assert len(self.cards()) == 3, self.cards()
        self.docker('stop', '--time', '10', self.prefix+'-'+old)
        self.docker('stop', '--time', '10', self.prefix+'-'+peer)
        after_plain = self.export_offline(old, 'after-plain-restart.json')
        after_handoff = self.export_offline(peer, 'after-new-protocol-handoff.json')
        stable_fields = ('id', 'intake_id', 'state', 'attempts', 'attempt_id', 'outcome', 'evidence',
                         'receiver', 'channel', 'destination', 'original_destination', 'content_revision',
                         'profile_id', 'profile_version', 'card_owner_id', 'payload')
        for label, snapshot in [('plain_restart', after_plain), ('new_handoff', after_handoff)]:
            unknown = self.target_for(snapshot, 'ordinary-upgrade-unknown')
            assert all(unknown.get(key) == unknown_before.get(key) for key in stable_fields), (label, unknown_before, unknown)
            pending = self.target_for(snapshot, 'ordinary-upgrade-pending')
            assert pending['id'] == pending_before['id'] and pending['state'] == 'delivered', (label, pending)
            legacy = [row for row in snapshot['intakes'] if row['id'] in {r['id'] for r in before['intakes']}]
            assert len(legacy) == 2 and all(row.get('identity_origin') == 'legacy-unsequenced' and
                not row.get('generation') and not row.get('reception_sequence') and not row.get('reception_instance')
                for row in legacy), (label, legacy)
        cards = self.cards()
        titles = [self.content(row)['header']['title']['content'] for row in cards]
        assert titles == ['ordinary-upgrade-unknown firing', 'ordinary-upgrade-pending firing',
                          'ordinary-upgrade-next-generation firing'], titles
        assert all(row['body'].get('receive_id', row['body'].get('chat_id')) == TEST for row in cards), cards
        self.event('new_protocol_after_legacy_restart', status='PASS', accepted_tasks=3,
                   platform_attempts=3, delivered_receipts=2, preserved_unknown_attempts=1,
                   replayed_old_unknown_attempts=0, missing=0, unexpected_duplicates=0,
                   unknown_target_id=unknown_before['id'], unknown_attempt_id=unknown_before['attempt_id'],
                   pending_target_id=pending_before['id'], legacy_records=2,
                   old_generations_fabricated=False, original_local_bolt_preserved=True,
                   runtime_new_binary_sha256=hashlib.sha256(self.args.binary.read_bytes()).hexdigest())

    def finish(self, error=None):
        self.stop_sampling.set()
        if hasattr(self, 'sampler'):
            self.sampler.join(timeout=5)
        for name in self.names:
            result = self.docker('logs', name, check=False)
            (self.root/(name+'.log')).write_text(result.stdout+result.stderr)
        (self.root/'resource-samples.json').write_text(json.dumps(self.samples, indent=2)+'\n')
        result = dict(status='FAIL' if error else 'PASS', scenario='plain-c62-restart-then-new-bluegreen',
                      duration_seconds=round(time.monotonic()-self.begin, 3),
                      binary_sha256=hashlib.sha256(self.args.binary.read_bytes()).hexdigest(),
                      old_image=OLD, error=str(error) if error else None, events=self.events,
                      docker_internal_network=self.net, host_ports=[],
                      production_or_uat_access=False, kubernetes_network_tested=False)
        for name in reversed(self.names):
            self.docker('rm', '-f', name, check=False)
        self.docker('network', 'rm', self.net, check=False)
        result['cleanup_complete'] = self.prefix not in self.docker('ps', '-a', '--format', '{{.Names}}').stdout
        (self.root/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--recovery', type=Path, required=True, help='native offline read-only ledger exporter')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.keep = False
    args.scenario = 'c62-restart'
    args.output.mkdir(parents=True, exist_ok=True)
    copied = args.output/'tested-notification-manager'
    shutil.copy2(args.binary, copied)
    args.binary = copied
    fixture = RestartFixture(args)
    error = None
    try:
        fixture.execute()
    except Exception as caught:
        error = caught
    finally:
        fixture.finish(error)
    if error:
        raise SystemExit(str(error))


if __name__ == '__main__':
    main()
