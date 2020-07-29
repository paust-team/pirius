#!/bin/bash

set -u

KF_BIN_PATH=$1

bash "$KF_BIN_PATH"/kafka-server-start.sh server.properties
