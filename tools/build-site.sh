#!/bin/bash

if [ -z "$1" ]; then
  echo "usage: build-site.sh microraft-version"
  exit 1
fi

yell() { echo "$0: $*" >&2; }
die() {
  yell "$*"
  exit 1
}
try() { "$@" || die "cannot $*"; }

MICRORAFT_VERSION=$1
POM_FILE=pom.xml
JAVADOC_SOURCE=microraft/target/site/apidocs
SITE_FILES_DIR=site-src
SITE_DIR=${SITE_FILES_DIR}/site
JAVADOC_TARGET=${SITE_DIR}/javadoc/${MICRORAFT_VERSION}
[[ -z "${MKDOCS_ENV}" ]] && MKDOCS_CMD='mkdocs' || MKDOCS_CMD="${MKDOCS_ENV}"
BUILD_SITE_CMD="${MKDOCS_CMD} build"

if [ ! -f "$POM_FILE" ]; then
  echo "Please run this script on the root directory of the MicroRaft repository."
  exit 1
fi

try mvn clean javadoc:javadoc
try test -d ${JAVADOC_SOURCE}

rm -rf $SITE_DIR
try cd $SITE_FILES_DIR
try $BUILD_SITE_CMD
cd ..
try test -d ${SITE_DIR}

try mkdir -p $JAVADOC_TARGET
try cp -avr ${JAVADOC_SOURCE}/* $JAVADOC_TARGET
try test -f ${JAVADOC_TARGET}/index.html

ls -l $SITE_DIR

echo "All good."
