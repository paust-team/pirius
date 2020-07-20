#!/bin/sh

set -u

zkhost=$1
zkport=$2

res=$( echo ruok | nc $zkhost $zkport )


while [ "$res" != "imok" ]
do
  echo "waiting for zookeeper run" 
  sleep 1
  res=$(echo ruok | nc $zkhost $zkport)
done
/go/bin/shapleq start -z "$zkhost:$zkport"
