# Build the snowball collector. Run from the repository root so the whole Go
# module is in the build context:
#
#   docker build -f cmd/snowball/Dockerfile -t snowball .
#
# modernc.org/sqlite is pure Go and top_ouis.txt is go:embed-ed, so the binary
# is fully static (CGO disabled) and has no runtime file dependencies.

FROM golang:1.23-alpine AS build
WORKDIR /src

# Download dependencies first so they cache across source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /snowball ./cmd/snowball

FROM alpine:3.20
# ca-certificates is needed for the HTTPS calls to Apple's WLOC API.
RUN apk add --no-cache ca-certificates \
 && adduser -S -D -H -h /data snowball

COPY --from=build /snowball /usr/local/bin/snowball

# SQLite database lives here; mount a volume to persist it across restarts.
RUN mkdir -p /data && chown snowball /data
VOLUME /data
WORKDIR /data
EXPOSE 8080
USER snowball

ENTRYPOINT ["snowball"]
CMD ["-db", "/data/snowball.db"]
