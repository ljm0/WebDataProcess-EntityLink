#!/usr/bin/env bash
if [ $1 == "help" ]; then
	echo $0: Usage: \<Mode - WARC or ARTICLE\> \<If ARTICLE - News Date Y/M/D, If WARC - Input-File-Path\> \<1 Entities - 2 Full text\> \<Number of topics\> \<Output directory\>
	exit 1
fi

if [ $# -ne 5 ]; then
	echo $0: Usage: \<Mode - WARC or ARTICLE\> \<If ARTICLE - News Date Y/M/D, If WARC - Input-File-Path\> \<1 Entities - 2 Full text\> \<Number of topics\> \<Output directory\>
	exit 1
fi

if [ "$SPARK_HOME" = "" ]; then
	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
	exit 1
fi

PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a2_TopicModelling.py $1 $2 $3 $4 $5