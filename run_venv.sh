#!/usr/bin/env bash

# Usage: bash spark_venv.sh [INFILE] [OUTFILE]

hdfs dfs -rm -r /user/wdps1811/sample

# zip code
zip code.zip html2text.py
zip code.zip nlp_preproc_spark.py
zip code.zip elasticsearch.py
zip code.zip sparql.py

# This assumes there is a python virtual environment in the "venv" directory
source venv/bin/activate
virtualenv --relocatable venv
zip -r venv.zip venv


### setup elasticsearch
ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

>.es_log*
prun -o .es_log -v -t 00:15:00  -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"


### setup sparql
KB_PORT=9090
KB_BIN=/home/bbkruit/scratch/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

prun -o .kb_log -v -t 00:15:00 -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "waiting 5 seconds for trident to set up..."
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
KB_PID=$!
echo "trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"




SCRIPT=${1:-"starter-code-spark.py"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTFILE=${3:-"sample"}

PYSPARK_PYTHON=$(readlink -f $(which python)) /home/bbkruit/spark-2.1.2-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/venv/bin/python3 \
--conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
--conf spark.yarn.appMasterEnv.NLTK_DATA=./NLTK_DATA/ \
--conf spark.executorEnv.NLTK_DATA=./NLTK_DATA/ \
--master yarn \
--deploy-mode cluster \
--num-executors 16 \
--executor-memory 4G \
--archives venv.zip#VENV,nltk_data.zip#NLTK_DATA,stanford-ner-2018-10-16.zip#NER \
--py-files code.zip \
$SCRIPT $INFILE $OUTFILE $ES_NODE:$ES_PORT $KB_NODE:$KB_PORT

# kill elasticsearch
kill $ES_PID
kill $KB_PID

# hdfs dfs -cat $OUTFILE"/*" > $OUTFILE
