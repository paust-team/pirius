#!/bin/sh

set -u

zkhost=$1
zkport=$2
configpath=$3
res=$( echo ruok | nc $zkhost $zkport )


while [ "$res" != "imok" ]
do
  echo "waiting for zookeeper run" 
  sleep 1
  res=$(echo ruok | nc $zkhost $zkport)
done
sleep 3
/go/bin/shapleq start --zk-host "$zkhost" --zk-port "$zkport" -i "$configpath"
