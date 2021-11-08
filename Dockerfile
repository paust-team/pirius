FROM golang:1.17-alpine AS builder

RUN apk update && apk add build-base git \
gcc file cmake autoconf automake libtool curl make linux-headers zlib-dev \
g++ unzip dep snappy-dev snappy

ENV USER=shapleuser
ENV UID=11010
ENV GID=11011

RUN addgroup --gid "$GID" "$USER" \
    && adduser \
    --disabled-password \
    --gecos "" \
    --home "$(pwd)" \
    --ingroup "$USER" \
    --no-create-home \
    --uid "$UID" \
    "$USER"

ENV SHPALEQ_DIR=$GOPATH/src/github.com/paust-team/shapleq
WORKDIR ${SHPALEQ_DIR}
RUN uname -a

COPY . .
RUN make build

FROM alpine:latest
RUN apk add --no-cache libstdc++ snappy

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/src/github.com/paust-team/shapleq/thirdparty/rocksdb/build/librocksdb.* /usr/local/lib/
COPY --from=builder /go/src/github.com/paust-team/shapleq/broker/cmd/shapleq/shapleq /go/bin/shapleq
COPY --from=builder /go/src/github.com/paust-team/shapleq/start-shapleq.sh /go/bin/start-shapleq.sh

WORKDIR /go/bin
RUN chown -R shapleuser /go
USER shapleuser:shapleuser
EXPOSE 1101
ENV ZK_ADDR=127.0.0.1
ENV ZK_PORT=2181
ENTRYPOINT ["sh", "-c", "/go/bin/start-shapleq.sh $ZK_ADDR $ZK_PORT /go/src/github.com/paust-team/shapleq/config.yml"]
