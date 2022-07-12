FROM golang:1.18-alpine3.16 AS builder

RUN apk update && apk add build-base git \
  gcc file cmake autoconf automake libtool curl make linux-headers zlib-dev \
  g++ unzip dep bash coreutils zstd-dev snappy-dev lz4-dev

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

ENV SHPALEQ_DIR=/shapleq
WORKDIR ${SHPALEQ_DIR}
RUN uname -a

COPY . .
RUN go mod download &&\
    git submodule update --init --recursive &&\
    make clean install DEPLOY_TARGET=release &&\
    cp -rf ~/.shapleq /shapleq/.shapleq

FROM alpine:3.16
RUN apk add --no-cache libstdc++ bash snappy &&\
    mkdir /shapleq

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /usr/local/bin/shapleq /shapleq/bin/shapleq
COPY --from=builder /usr/local/bin/shapleq-client /shapleq/bin/shapleq-client
COPY --from=builder /shapleq/start-shapleq.sh /shapleq/start-shapleq.sh
COPY --from=builder /shapleq/.shapleq/config /shapleq/config

WORKDIR /shapleq
RUN chown -R shapleuser /shapleq &&\
    ls -al /shapleq/config/broker
USER shapleuser:shapleuser
EXPOSE 1101
ENV ZK_QUORUM="127.0.0.1:2181" HOME=/shapleq
ENTRYPOINT ["bash", "-c", "./start-shapleq.sh $ZK_QUORUM /shapleq/config/broker/config.yml"]
