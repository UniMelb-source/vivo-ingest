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
    java -jar VivoIngest.jar -action add -addFileName ${TTL} -dbString ${DB_STRING} -localModelName ${FAE_MODEL} -password ${PASSWORD} -userName ${USERNAME} -jenaType ${JENATYPE}
done
rm -rf ${TEMPDIR}
