#!/bin/bash

. config.sh

DELTA_BASE="/mnt/storage/Downloads/ttl/www.findanexpert.unimelb.edu.au/vivoDeltas/"

for DELTA in ${DELTA_BASE}*DEL*ttl
do
	java -jar VivoIngest.jar -action remove -removeFileName ${DELTA} -dbString ${DB_STRING} -localModelName ${DB_REMOTE_MODEL} -password ${DB_PASSWORD} -userName ${DB_USERNAME} -jenaType ${DB_JENATYPE}
done
