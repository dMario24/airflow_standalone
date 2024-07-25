#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

user="root"

MYSQL_PWD='qwer123' mysql --local-infile=1 -u"$user"<<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
FIELDS 
	TERMINATED BY ',' 
	ENCLOSED BY '^' 
	ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF