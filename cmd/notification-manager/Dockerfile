ARG GOPROXY="https://goproxy.io"
# Build the manager binary
FROM golang:1.23.12 AS builder
ARG GOPROXY

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN GOPROXY=$GOPROXY go mod download

# Copy the go source
COPY cmd/notification-manager/main.go main.go
COPY cmd/jev-annotation-executor/ cmd/jev-annotation-executor/
COPY cmd/notification-spool-recovery/ cmd/notification-spool-recovery/
COPY cmd/operator/main.go cmd/operator/main.go
COPY controllers/ controllers/
COPY pkg/ pkg/
COPY apis/ apis/

# Build
RUN CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags="-s -w" -o notification-manager main.go \
 && CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags="-s -w" -o jev-annotation-executor ./cmd/jev-annotation-executor \
 && CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags="-s -w" -o notification-spool-recovery ./cmd/notification-spool-recovery \
 && CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags="-s -w" -o notification-manager-operator ./cmd/operator

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM kubesphere/distroless-static:nonroot
WORKDIR /
COPY --from=builder /workspace/notification-manager .
COPY --from=builder /workspace/jev-annotation-executor .
COPY --from=builder /workspace/notification-spool-recovery .
COPY --from=builder /workspace/notification-manager-operator .
USER nonroot:nonroot

ENTRYPOINT ["/notification-manager"]
