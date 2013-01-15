#!/bin/bash

function clean_up_file {
    FILE=$1 
    echo "Cleaning up basic TTL errors for ${FILE}"
    sed -i '/^<[^>]*><[^>]*>\.$/d' ${FILE}
    sed -i '/^[^<].*$/d' ${FILE}
    sed -i '/^$/d' ${FILE}
} 
