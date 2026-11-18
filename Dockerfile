ARG BASE_IMAGE=registry.access.redhat.com/ubi9/ubi-minimal:latest

FROM registry.access.redhat.com/ubi9/go-toolset:9.8-1793736700 AS builder

ARG GIT_SHA=unknown
ARG BUILD_DATE=""
ARG APP_VERSION="0.0.0-dev"

# Create /build as root and hand it to the non-root build user.
USER root
WORKDIR /build
RUN chown 1001:0 /build
USER 1001

ENV GOBIN=/build/.gobin
RUN mkdir -p $GOBIN
ENV PATH="${GOBIN}:${PATH}"

COPY --chown=1001:0 go.mod go.sum ./
RUN --mount=type=cache,target=/opt/app-root/src/go/pkg/mod,uid=1001 \
    go mod download

COPY --chown=1001:0 . .

# Enable FIPS only in the image: select the validated module and include fips.go.
# no_openssl selects Go's native backend on the Red Hat toolset.
RUN --mount=type=cache,target=/opt/app-root/src/go/pkg/mod,uid=1001 \
    --mount=type=cache,target=/opt/app-root/src/.cache/go-build,uid=1001 \
    GOOS=linux GOFIPS140=v1.0.0 \
    GIT_SHA=${GIT_SHA} BUILD_DATE=${BUILD_DATE} APP_VERSION=${APP_VERSION} \
    make build GOFLAGS="-trimpath -tags=fips,no_openssl"

# Check the selected module, compiled default, and startup on a simulated FIPS node.
RUN go version -m /build/bin/hyperfleet-applier | grep -Eq 'GOFIPS140=v1[.]0[.]0(-[[:xdigit:]]+)?$' || { echo "FIPS check FAILED: missing GOFIPS140=v1.0.0"; exit 1; }
RUN go version -m /build/bin/hyperfleet-applier \
    | grep -E '^[[:space:]]*build[[:space:]]+DefaultGODEBUG=([^,]*,)*fips140=on(,|$)' \
    | grep -Ev 'DefaultGODEBUG=([^,]*,)*fips140ems=0(,|$)' \
    || { echo "FIPS check FAILED: require fips140=on without fips140ems=0"; exit 1; }
RUN GOLANG_FIPS=1 /build/bin/hyperfleet-applier --help >/dev/null || { echo "FIPS check FAILED: binary does not start in FIPS mode"; exit 1; }

# Runtime stage
FROM ${BASE_IMAGE}

WORKDIR /app

COPY --from=builder /build/bin/hyperfleet-applier /app/hyperfleet-applier
COPY --from=builder /build/LICENSE /licenses/LICENSE

USER 65532:65532

ENTRYPOINT ["/app/hyperfleet-applier"]

ARG APP_VERSION="0.0.0-dev"
LABEL name="hyperfleet-applier" \
      vendor="Red Hat, Inc." \
      version="${APP_VERSION}" \
      summary="HyperFleet Applier" \
      description="Controller responsible for Hyperfleet ReadDesires, ApplyDesires and DeleteDesires"
