#!/usr/bin/env bash

export LEIN_SNAPSHOTS_IN_RELEASE=true

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR/../kafka-clj
lein do clean, install, midje

