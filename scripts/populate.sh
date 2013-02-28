#!/bin/bash

. config.sh
. include.sh

FILE_LATEST="ingest.latest"
DAYSEC=86400
URL_BASE="http://www.findanexpert.unimelb.edu.au/vivoDeltas/"

JAVA_ARGS="-Xmx3072m" 

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

	POPULATE_ARGS="-action add -addFileName ${ADD_ABS_FILE} -dbString ${DB_STRING} -localModelName ${DB_REMOTE_MODEL} -password ${DB_PASSWORD} -userName ${DB_USERNAME} -jenaType ${DB_JENATYPE}"
	DEPOPULATE_ARGS="-action remove -removeFileName ${DEL_ABS_FILE} -dbString ${DB_STRING} -localModelName ${DB_REMOTE_MODEL} -password ${DB_PASSWORD} -userName ${DB_USERNAME} -jenaType ${DB_JENATYPE}"
	echo ${CURRENT_DATE} > ${FILE_LATEST}
	LATEST_TS=$((${LATEST_TS} + ${DAYSEC}))

	echo "Fetching add file"
	wget -cnv ${ADD_URL} -O ${ADD_ABS_FILE}
	RETURN=$?
	if [ ${RETURN} -gt 0 ]
	then
		echo "Unable to fetch file: ${RETURN}"
	else	
		echo "Adding file"
		clean_up_file ${ADD_ABS_FILE}
		java ${JAVA_ARGS} -jar VivoIngest.jar ${POPULATE_ARGS}
		cat construct-add.ttl >> ${CURRENT_DATE}-construct-add.ttl
		rm construct-add.ttl
		cat construct-remove.ttl >> ${CURRENT_DATE}-construct-remove.ttl
		rm construct-remove.ttl
		bzip2 ${ADD_ABS_FILE}
	fi

	echo "Fetching del file"
	wget -cnv ${DEL_URL} -O ${DEL_ABS_FILE}
	RETURN=$?
	if [ ${RETURN} -gt 0 ]
	then
		echo "Unable to fetch file: ${RETURN}"
	else	
		echo "Deleting file"
		clean_up_file ${DEL_ABS_FILE}
		java ${JAVA_ARGS} -jar VivoIngest.jar ${DEPOPULATE_ARGS}
		bzip2 ${DEL_ABS_FILE}
	fi
done
