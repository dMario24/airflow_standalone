#!/bin/bash

FILE_PATH=$1

if [[ -e  $FILE_PATH ]]
then
        echo "File exist"
        exit 0
else
       echo "File not found"
       exit 2
fi
