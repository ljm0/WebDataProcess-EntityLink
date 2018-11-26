#!/usr/bin/env bash
#./run.sh WARC-TREC-ID hdfs:///user/bbkruit/sample.warc.gz

# if [ $# -ne 2 ]; then
# 	echo $0: Usage: \<Warc-key\> \<Input-File-Path\> 
# 	exit 1
# fi

if [ "$SPARK_HOME" = "" ]; then
	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
	exit 1
fi

# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py $1 $2
PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py "WARC-TREC-ID" "/home/wdps1811/scratch/ljm_workspace/Large_Scale_Entity_Linking/data/sample.warc.gz"