#!/usr/bin/env bash

if [ -z "$1" ]; then
	echo "usage: repeat.sh repeat_count optional_test_name"
	exit 1
fi

if [ -n "$2" ]; then
  TEST_NAME=$2
  echo "TEST NAME: $TEST_NAME"
fi

REPEAT=$1
ROUND="1"

./gradlew clean

while [ ${ROUND} -le "${REPEAT}" ]; do

    echo "ROUND: $ROUND"

    if [ -n "$TEST_NAME" ]; then
      ./gradlew test --tests "$TEST_NAME"
    else
      ./gradlew test
    fi

    if [ $? != '0' ]; then
        echo "test failed"
        exit 1
    fi

    ROUND=$(expr $ROUND \+ 1)

done
