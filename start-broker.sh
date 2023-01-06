#!/bin/sh

set -u

zkquorum=$1
configpath=$2
sleep 3
bin/pirius-broker start --zk-quorum "$zkquorum" -i "$configpath"
