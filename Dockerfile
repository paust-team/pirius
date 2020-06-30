FROM golang:1.13-alpine AS builder

RUN apk update && apk add build-base git \
gcc file cmake autoconf automake libtool curl make linux-headers zlib-dev \
g++ unzip dep snappy-dev

ENV USER=shapleuser
ENV UID=11001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

ENV PAUSTQ_DIR=$GOPATH/src/github.com/paust-team/paustq
WORKDIR ${PAUSTQ_DIR}
RUN uname -a

COPY . .
RUN make build

FROM scratch
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/src/github.com/paust-team/paustq/broker/cmd/paustq/shapleq /go/bin/shapleq
USER appuser:appuser
ENTRYPOINT ["/go/bin/shapleq"]
