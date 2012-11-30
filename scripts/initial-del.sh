#!/bin/bash

. config.sh

INITIAL_TTL=$(readlink -f $1)
TEMPDIR=$(mktemp -d)
LINES_PER_FILE=50000

pushd ${TEMPDIR}
split -l ${LINES_PER_FILE} ${INITIAL_TTL}
popd
for TTL in ${TEMPDIR}/*
do
    java -jar VivoIngest.jar -action remove -removeFileName ${TTL} -dbString ${DB_STRING} -localModelName ${DB_REMOTE_MODEL} -password ${DB_PASSWORD} -userName ${DB_USERNAME} -jenaType ${DB_JENATYPE}
done
rm -rf ${TEMPDIR}
