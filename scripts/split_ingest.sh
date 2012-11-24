#!/bin/bash

TTL=$(readlink -f $1)
DB_STRING=$2
DB_MODEL=$3
DB_PASSWORD=$4
DB_USERNAME=$5
DB_JENATYPE=$6
DB_ACTION=$7
TEMPDIR=$(mktemp -d)
LINES_PER_FILE=50000

pushd ${TEMPDIR}
split -l ${LINES_PER_FILE} ${TTL}
popd
for TTL_SPLIT in ${TEMPDIR}/*
do
    java -jar VivoIngest.jar -action ${DB_ACTION} -addFileName ${TTL_SPLIT} -dbString ${DB_STRING} -localModelName ${DB_MODEL} -password ${DB_PASSWORD} -userName ${DB_USERNAME} -jenaType ${DB_JENATYPE}
done
rm -rf ${TEMPDIR}
