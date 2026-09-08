# Broker image. The harness is not containerised: it runs on developer
# machines next to their Claude login.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X github.com/bambamboole/mattermost-harness-bridge/internal/version.Version=$VERSION" \
    -o /out/broker ./cmd/broker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/broker /broker
ENV DB_PATH=/data/broker.db LISTEN_ADDR=:8080
VOLUME /data
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/broker"]
