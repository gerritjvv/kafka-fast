#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR

function fail (){
 echo "Stage $1 :: $2 failed"
 exit -1
}

export LEIN_SNAPSHOTS_IN_RELEASE=true

echo "Running check"
lein do clean, check || fail "CHECK" "check" 

echo "Running tests"
for i in $DIR/test/kafka_clj/*tests.clj; do
  testname="kafka_clj.$(basename ${i%.*})"
  echo $testname
  lein midje `echo $testname | sed 's;_;-;g'` || fail "TEST" $testname
done

echo "Installing"

lein install 

