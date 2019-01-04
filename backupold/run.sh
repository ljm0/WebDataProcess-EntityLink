#!/usr/bin/env bash
#./run.sh WARC-TREC-ID hdfs:///user/wdps1811/sample.warc.gz

# if [ $# -ne 2 ]; then
# 	echo $0: Usage: \<Warc-key\> \<Input-File-Path\> 
# 	exit 1
# fi

ES_PORT=9200
# ES_BIN=/home/bbkruit/scratch/wdps/elasticsearch-2.4.1/bin/elasticsearch
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
sleep 15
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

SCRIPT=${1:-"a1.py"}
WARCID=${2:-"WARC-TREC-ID"}
INFILE=${3:-"hdfs:///user/wdps1811/sample.warc.gz"}
OUTFILE=${4:-"templeResultLIU02"}
ELASTICSEARCH=${5:-"$ES_NODE:$ES_PORT"}

# if [ "$SPARK_HOME" = "" ]; then
# 	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
# 	exit 1ls

# fi
export PYTHONPATH=$PYTHONPATH:/home/wdps1811/scratch/lib
# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py $1 $2
# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py "WARC-TREC-ID" "hdfs:///user/wdps1811/sample.warc.gz"
PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] $SCRIPT $WARCID $INFILE $OUTFILE $ELASTICSEARCH

hdfs dfs -cat $OUTFILE"/*" > $OUTFILE
# remove the result to project generating same name file
hdfs dfs -rm -r  hdfs:///user/wdps1811/$OUTFILE
