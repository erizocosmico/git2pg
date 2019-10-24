FROM golang:1.13-alpine AS builder

RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /git2pg/
COPY . .

RUN go mod download
RUN go mod verify
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/git2pg ./cmd/git2pg/main.go

FROM alpine:3.8

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /bin/git2pg /bin/git2pg

RUN adduser -D -g '' git2pg
USER git2pg

ENTRYPOINT ["/bin/git2pg", "-d", "/repositories"]
