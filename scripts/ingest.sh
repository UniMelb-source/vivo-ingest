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

WORKING_DIR=/var/lib/rdr-unimelb/ingest

while [ ${LATEST_TS} -lt ${NOW} ]
do
	CURRENT_DATE=$(date -d @${LATEST_TS} +%Y%m%d)

	echo "Processing ${CURRENT_DATE}"
	ADD_FILE=$(date -d @${LATEST_TS} +%Y%m%d)_ADD.ttl
	ADD_ABS_FILE=${WORKING_DIR}/${ADD_FILE}
	ADD_URL=${URL_BASE}${ADD_FILE}

	DEL_FILE=$(date -d @${LATEST_TS} +%Y%m%d)_DEL.ttl
	DEL_ABS_FILE=${WORKING_DIR}/${DEL_FILE}
	DEL_URL=${URL_BASE}${DEL_FILE}

	INGEST_ARGS="-action process -addFileName ${ADD_ABS_FILE} -removeFileName ${DEL_ABS_FILE} -jenaType ${DB_JENATYPE} -dbString ${DB_STRING} -userName ${DB_USERNAME} -password ${DB_PASSWORD} -localModelName ${DB_LOCAL_MODEL} -remoteModelName ${DB_REMOTE_MODEL}"
	echo ${CURRENT_DATE} > ${FILE_LATEST}
	LATEST_TS=$((${LATEST_TS} + ${DAYSEC}))

	echo "Fetching add files"
	wget -cnv ${ADD_URL} -O ${ADD_ABS_FILE}
	ADD_RETURN=$?

	echo "Fetching del file"
	wget -cnv ${DEL_URL} -O ${DEL_ABS_FILE}
	DEL_RETURN=$?

	if [ ${ADD_RETURN} -gt 0 -a ${DEL_RETURN} -gt 0 ]
	then
		echo "Unable to fetch files"
		rm -f ${ADD_ABS_FILE}
		rm -f ${DEL_ABS_FILE}
		continue
	fi

	if [ ${ADD_RETURN} -gt 0 ]
	then
		touch ${ADD_ABS_FILE}
	else
		clean_up_file ${ADD_ABS_FILE}
	fi

	if [ ${DEL_RETURN} -gt 0 ]
	then
		touch ${DEL_ABS_FILE}
	else
		clean_up_file ${DEL_ABS_FILE}
	fi

	echo "Running ingest"
	java ${JAVA_ARGS} -jar VivoIngest.jar ${INGEST_ARGS}
	mv children.ttl ${WORKING_DIR}/${CURRENT_DATE}-children.ttl
	mv construct-add.ttl ${WORKING_DIR}/${CURRENT_DATE}-construct-add.ttl
	mv construct-remove.ttl ${WORKING_DIR}/${CURRENT_DATE}-construct-remove.ttl
	echo "Ingest done"
	bzip2 ${ADD_ABS_FILE}
	bzip2 ${DEL_ABS_FILE}
done
