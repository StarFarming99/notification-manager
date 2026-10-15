# Isolated local-ledger release test

Run `bluegreen_isolated.py --help`. Supply a CGO-disabled `linux/amd64` NM binary
and a new evidence directory. This is a local development runner, not a script
for a production/JMS host. It requires Python's standard library, Docker and
OpenSSL; containers do not have host ports or access to a real cluster/platform.
Image pulls are disabled. Preload the pinned c62 image, the verified rc7 fixture
image, `python:3-alpine` and `prom/alertmanager:v0.23.0` before running offline.

The runner starts real executables with independent Bolt volumes, synthetic
read-only Kubernetes resources/Secret references, a local TLS platform and real
AM peers. It tests forward and reverse handoff, retired restart, lifecycle order,
peer overlap, legitimate repeat, actual synchronous success/failure/timeout,
standby rejection, unknown persistence and release rejection. Every send is
counted at the platform; ledger alias counts are not delivery proof.

The native AM fixture uses a Service proxy, not a real Kubernetes Service. NM
limits are 800m CPU/1 GiB, matching the repository's original infra-devops
configuration; they are not a fresh observation of production. The artificial
AM repeat interval is eight seconds, not a twelve-hour production soak. Resource
samples, event reconciliation, send-delay upper bounds and logs are retained.

The c62 image tests its actual missing online protocol and preserved base sender.
Original synchronous API semantics are compared against the separately verified
rc7 executable. c62's unprofiled durable APIs already returned queue acceptance;
that behavior is not the original synchronous success contract being preserved.
Full migration acceptance remains BLOCKED by c62 bootstrap and indistinguishable
same-start AM generations. This runner never authorizes a deployment or takeover.
