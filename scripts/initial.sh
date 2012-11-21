#!/bin/bash
DB_STRING="jdbc:mysql://localhost/vivo"
MODEL="http://vitro.mannlib.cornell.edu/default/vitro-kb-2"
USERNAME="vivo"
PASSWORD="vivo"
JENATYPE="SDB"
INITIAL_TTL=$(readlink -f $1)
TEMPDIR=$(mktemp -d)
LINES_PER_FILE=50000

pushd ${TEMPDIR}
split -l ${LINES_PER_FILE} ${INITIAL_TTL}
popd
for TTL in ${TEMPDIR}/*
do
    java -jar VivoIngest.jar -action add -addFileName ${TTL} -dbString ${DB_STRING} -localModelName ${MODEL} -password ${PASSWORD} -userName ${USERNAME} -jenaType ${JENATYPE}
done
rm -rf ${TEMPDIR}
