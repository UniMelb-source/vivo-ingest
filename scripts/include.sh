#!/bin/bash

function clean_up_file {
    FILE=$1 
    echo "Cleaning up basic TTL errors for ${FILE}"
    sed -i -e '/^<[^>]*><[^>]*>\.$/d' -e '/^[^<].*$/d' -e '/^$/d' ${FILE}
} 
