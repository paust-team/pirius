FROM golang:1.18-alpine3.16 AS builder

RUN apk update && apk add build-base git \
  gcc file cmake autoconf automake libtool curl make linux-headers zlib-dev \
  g++ unzip dep bash coreutils zstd-dev snappy-dev lz4-dev

ENV USER=piriususer
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

ENV HOME_DIR=/pirius
WORKDIR ${HOME_DIR}
RUN uname -a

COPY . .
RUN go mod download &&\
    git submodule update --init --recursive &&\
    make clean install DEPLOY_TARGET=release &&\
    cp -rf ~/.pirius /pirius/.pirius

FROM alpine:3.16
RUN apk add --no-cache libstdc++ bash snappy &&\
    mkdir /pirius

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /usr/local/bin/pirius-broker /pirius/bin/pirius-broker
COPY --from=builder /pirius/start-broker.sh /pirius/start-broker.sh
COPY --from=builder /pirius/.pirius/config /pirius/config

WORKDIR /pirius
RUN chown -R piriususer:piriususer /pirius && chmod -R 777 /pirius
USER piriususer:piriususer
EXPOSE 1101
ENV ZK_QUORUM="127.0.0.1:2181" HOME=/pirius
ENTRYPOINT ["bash", "-c", "./start-broker.sh $ZK_QUORUM /pirius/config/broker/config.yml"]
