#!/usr/bin/env bash
#./run.sh WARC-TREC-ID hdfs:///user/wdps1811/sample.warc.gz

# if [ $# -ne 2 ]; then
# 	echo $0: Usage: \<Warc-key\> \<Input-File-Path\> 
# 	exit 1
# fi

ES_PORT=9200
ES_BIN=/home/bbkruit/scratch/wdps/elasticsearch-2.4.1/bin/elasticsearch

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
sleep 15
ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)')
ES_PID=$!
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

# sleep 30

# prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
# echo "waiting 15 seconds for elasticsearchBackup1 to set up..."
# sleep 15
# ES_NODE1=$(cat .es_node | grep '^:' | grep -oP '(node...)')
# ES_PID1=$!
# echo "elasticsearch_backup1 should be running now on node $ES_NODE1:$ES_PORT (connected to process $ES_PID1)"

# sleep 60

# prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
# echo "waiting 15 seconds for elasticsearchBackup2 to set up..."
# sleep 15
# ES_NODE2=$(cat .es_node | grep '^:' | grep -oP '(node...)')
# ES_PID2=$!
# echo "elasticsearch_backup2 should be running now on node $ES_NODE2:$ES_PORT (connected to process $ES_PID2)"

SCRIPT=${1:-"a1.py"}
WARCID=${2:-"WARC-TREC-ID"}
INFILE=${3:-"hdfs:///user/wdps1811/sample.warc.gz"}
OUTFILE=${4:-"templeResultLIU"}
ELASTICSEARCH=${5:-"$ES_NODE:$ES_PORT"}
# ELASTICSEARCH1=${6:-"$ES_NODE:$ES_PORT"}
# ELASTICSEARCH2=${7:-"$ES_NODE:$ES_PORT"}

if [ "$SPARK_HOME" = "" ]; then
	echo SPARK_HOME not set. Please create the environment variable SPARK_HOME pointing to your Spark home directory.
	exit 1ls

fi
export PYTHONPATH=$PYTHONPATH:/home/wdps1811/scratch/lib
# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py $1 $2
# PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] a1.py "WARC-TREC-ID" "hdfs:///user/wdps1811/sample.warc.gz"
PYSPARK_PYTHON=$(readlink -f $(which python)) $SPARK_HOME/bin/spark-submit --master local[*] $SCRIPT $WARCID $INFILE $OUTFILE $ELASTICSEARCH
#$ELASTICSEARCH1 $ELASTICSEARCH2

hdfs dfs -cat $OUTFILE"/*" > $OUTFILE
# remove the result to project generating  same name file
hdfs dfs -rm -r  hdfs:///user/wdps1811/$OUTFILE
