#!/bin/bash
DELTA_BASE="/mnt/storage/Downloads/ttl/www.findanexpert.unimelb.edu.au/vivoDeltas/"
DB_STRING="jdbc:mysql://localhost/vivo"
MODEL="http://vitro.mannlib.cornell.edu/default/vitro-kb-2"
USERNAME="vivo"
PASSWORD="vivo"
JENATYPE="SDB"

for DELTA in ${DELTA_BASE}*DEL*ttl
do
	java -jar VivoIngest.jar -action add -addFileName ${DELTA} -dbString ${DB_STRING} -localModelName ${MODEL} -password ${PASSWORD} -userName ${USERNAME} -jenaType ${JENATYPE}
done
