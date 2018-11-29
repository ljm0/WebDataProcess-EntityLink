#!/usr/bin/env bash

#if [ $# -ne 2 ]; then
#	echo $0: Usage: \<Warc-key\> \<Input-File-Path\> 
#	exit 1
#fi

SCRIPT=${1:-"a1.py"}
WARCID=${2:-"WARC-TREC-ID"}
INFILE=${3:-"hdfs:///user/wdps1811/sample.warc.gz"}
OUTFILE=${4:-"templeResult"}

if [ "$SPARK_HOME" = "" ]; then
	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
	exit 1
fi
# Create virtual environment and install all necessary dependencies

source venv/bin/activate
virtualenv --relocatable venv
zip -r venv.zip venv

PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/venv/bin/python \
--master yarn \
--deploy-mode cluster \
--archives venv.zip#VENV \
$SCRIPT $WARCID $INFILE $OUTFILE

hdfs dfs -cat $OUTFILE"/*" > $OUTFILE


#A1_WDPS1708.py $1 $2
