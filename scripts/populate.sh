#!/bin/bash

. config.sh

DELTA_BASE="/mnt/storage/Downloads/ttl/www.findanexpert.unimelb.edu.au/vivoDeltas/"

for DELTA in ${DELTA_BASE}*DEL*ttl
do
	java -jar VivoIngest.jar -action add -addFileName ${DELTA} -dbString ${DB_STRING} -localModelName ${DB_REMOTE_MODEL} -password ${PASSWORD} -userName ${USERNAME} -jenaType ${JENATYPE}
done
