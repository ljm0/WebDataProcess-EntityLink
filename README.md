# Web_Data_Entity_Linking

This project is to perform Entity Linking on a collection of web pages.

## Description

This project is to perform Entity Linking on a collection of web pages. Our method consists of the following five steps:  

1. extract text from HTML pages in WARC files using beautifulsoups
2. tokenize each text and recognize named entities in the content using nltk
3. link each entity mention to a set of candidate entities in Freebase using ELASTICSEARCH  
   In this step, we will get a list of candidate Freebase IDs for each entity
4. query each candidate's abstract in DBPedia using SPARQL  
5. consider two similarities by computing cosine similarity using sklearn:   
   - the abstract and the text (where the entity mention retrieved from)  
   - the entity mention and the abstract's object (noun phrase before the first verb)  
   (sum of these two scores is candidate entity's similarity score)
   link the entity mention to the candidate entity with highest similarity score  
   return the result in the format: document IDs + '\t' + entity surface form + '\t' + Freebase entity ID

We also perform our method with Spark at cluster mode.

## Prerequisites

python-package: beautifulsoup4, nltk, sklearn, requests 

```bash
pip install -U beautifulsoup4 nltk scikit-learn requests
```

Stanford NER

```bash
wget https://nlp.stanford.edu/software/stanford-ner-2018-10-16.zip  
```

_path in DAS-4_
```bash
cd /home/wdps1811/scratch/wdps-group11
```

## How to run

run without Spark

```bash
# run
# SCRIPT: starter-code.py
# INPUT: hdfs:///user/bbkruit/sample.warc.gz
# OUTPUT: sample
bash run.sh
```

run with Spark

_setup environment_
```bash
# setup virtualenv
python3 -m venv venv
# download python-packages
source venv/bin/activate
export PYTHONPATH=""
pip install -U beautifulsoup4 nltk scikit-learn requests

# download nltk_data and zip it
python -m nltk.downloader -d ./ all
zip -r nltk_data.zip ./nltk_data
# download stanford-ner
wget https://nlp.stanford.edu/software/stanford-ner-2018-10-16.zip 
```

_run_
```bash
# run
# SCRIPT: starter-code-spark.py
# INPUT: hdfs:///user/bbkruit/sample.warc.gz
# OUTPUT: sample
bash run_venv.sh <SCRIPT> <INPUT> <OUTPUT>
```



compute F1-score

~~~bash
# if run with spark, the output is in hdfs
hdfs dfs -cat /user/wdps1811/sample/* > output.tsv
# compute F1-score
python score.py data/sample.annotations.tsv output.tsv
~~~



## Notes

1. _run.sh_: run at local  
   _run_venv.sh_: run at cluster

2. _starter-code.py_: main pipeline of Entity Linking and code for ranking candidate entities  
   _starter-code-spark.py_: use rdd operations to perform Entity Linking

3. _html2text.py_: warc -> html -> text  
   remove html tags and useless text (script, comment, code, style...)  
   get text in tag '<p></p>'  

4. _nlp_preproc.py_:text -> tokens -> clean_tokens(remove stopwords) -> ner_tagged_tokens  
   _nlp_preproc_spark.py_: use nltk ner to tag tokens

5. _elasticsearch.py_: candidate entities generation  
   search for candidate Freebase entities IDs by elasticsearch

6. _sparql.py_: query candidate entities' abstract

## Extension -  Detect the most salient entities

main idea: 

- the most salient entities are relevant to the title of HTML pages
- compute Jaccard similarity of entity’s abstract and title of the page

extension prerequisties: unidecode, numpy

```bash
pip install -U unidecode numpy
```
run
```bash
# SCRIPT: extension-starter-code.py
# INPUT: hdfs:///user/bbkruit/sample.warc.gz
bash entension_run.sh > <OUTPUT>
```

1. entension_run.sh: run extension
2. extension-starter-code.py: detect the most salient entities by computing jaccard_index
3. extensionhtml2text.py: get title
