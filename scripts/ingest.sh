#!/bin/bash

. config.sh
. include.sh

FILE_LATEST="ingest.latest"
DAYSEC=86400
URL_BASE="http://www.findanexpert.unimelb.edu.au/vivoDeltas/"

JAVA_ARGS="-Xmx2048m" 

NOW=$(date +%s)
LATEST_DATE=$(cat ${FILE_LATEST})
LATEST_TS=$(date -d ${LATEST_DATE} +%s)
LATEST_TS=$((${LATEST_TS} + ${DAYSEC}))

while [ ${LATEST_TS} -lt ${NOW} ]
do
	CURRENT_DATE=$(date -d @${LATEST_TS} +%Y%m%d)
	echo "Processing ${CURRENT_DATE}"
	ADD_FILE=$(date -d @${LATEST_TS} +%Y%m%d)_ADD.ttl
	ADD_URL=${URL_BASE}${ADD_FILE}
	DEL_FILE=$(date -d @${LATEST_TS} +%Y%m%d)_DEL.ttl
	DEL_URL=${URL_BASE}${DEL_FILE}
	INGEST_ARGS="-action process -addFileName ${ADD_FILE} -removeFileName ${DEL_FILE} -jenaType ${DB_JENATYPE} -dbString ${DB_STRING} -userName ${DB_USERNAME} -password ${DB_PASSWORD} -localModelName ${DB_LOCAL_MODEL} -remoteModelName ${DB_REMOTE_MODEL}"
	echo "Fetching TTL files"
	wget -cnv ${ADD_URL} && wget -cnv ${DEL_URL}
	RETURN=$?
	echo ${CURRENT_DATE} > ${FILE_LATEST}
	LATEST_TS=$((${LATEST_TS} + ${DAYSEC}))
	if [ ${RETURN} -gt 0 ]
	then
		echo "Unable to fetch files: ${RETURN}"
		continue
	fi
	clean_up_file ${ADD_FILE}
	clean_up_file ${DEL_FILE}
	echo "Running ingest"
	java ${JAVA_ARGS} -jar VivoIngest.jar ${INGEST_ARGS}
	mv children.ttl ${CURRENT_DATE}-children.ttl
	mv construct-add.ttl ${CURRENT_DATE}-construct-add.ttl
	mv construct-remove.ttl ${CURRENT_DATE}-construct-remove.ttl
	echo "Ingest done"
done
