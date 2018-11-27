#!/usr/bin/env bash
#./run.sh WARC-TREC-ID hdfs:///user/wdps1811/sample.warc.gz

# if [ $# -ne 2 ]; then
# 	echo $0: Usage: \<Warc-key\> \<Input-File-Path\> 
# 	exit 1
# fi

SCRIPT=${1:-"a1.py"}
WARCID=${2:-"WARC-TREC-ID"}
INFILE=${3:-"hdfs:///user/wdps1811/sample.warc.gz"}
OUTFILE=${4:-"templeResult"}

if [ "$SPARK_HOME" = "" ]; then
	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
	exit 1ls

fi

# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py $1 $2
# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py "WARC-TREC-ID" "hdfs:///user/wdps1811/sample.warc.gz"
PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] $SCRIPT $WARCID $INFILE $OUTFILE

hdfs dfs -cat $OUTFILE"/*" > $OUTFILE