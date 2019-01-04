from pyspark import SparkContext
import sys


# import collections
from html2text import html2text
from nlp_preproc_spark import nlp_preproc
from elasticsearch import search
from sparql import query_abstract
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

sc = SparkContext("yarn", "wdps1811")

KEYNAME = "WARC-TREC-ID"
INFILE = sys.argv[1]
OUTFILE = sys.argv[2]
ELASTICSEARCH = sys.argv[3]
SPARQL = sys.argv[4]

rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})


def find_key(payload):
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(":")[1]
            return key;
    return "";

vectorizer = TfidfVectorizer()
def cosine_sim(text1, text2):
        tfidf = vectorizer.fit_transform([text1, text2])
        return ((tfidf * tfidf.T).A)[0,1]

def ner_tagged_tokens(record):
    _,payload = record
    key = find_key(payload)
    text = None
    tagged_tokens = None
    if key != "":
        text = html2text(payload)
        tagged_tokens = nlp_preproc(text)
        for token in tagged_tokens:
            yield key,text,token[0]

rdd = rdd.flatMap(ner_tagged_tokens)

# dictionary for elasticsearch:{token: [candidate Freebase entity ID]}
entity_dict = {}
def search_candidate(token):
    entities = None
    if entity_dict.__contains__(token):
        entities = entity_dict[token]
    else:
        entities = search(ELASTICSEARCH,token).items()
        entity_dict[token] = entities
    return entities

# dictionary for sparql: {FreebaseID:abstract}
abstract_dict = {}
def query_candidate_abstract(entity):
    abstract = ""
    if abstract_dict.__contains__(entity):
        abstract = abstract_dict[entity]
    else:
        abstract = query_abstract(SPARQL, entity)
        abstract_dict[entity] = abstract
    return abstract

def candidate_entity_generation(record):
    key,text,token = record
    entities = search_candidate(token)
    entities_list = []
    for entity, labels in entities:
        abstract = query_candidate_abstract(entity)
        if abstract != None:
            entities_list.append([entity,abstract])
    if entities_list != []:
        yield key,text,token,entities_list

rdd = rdd.flatMap(candidate_entity_generation)

def find_abstract_object(abstract):
    abstract_token = nltk.word_tokenize(abstract)
    abstract_pos_tag = nltk.pos_tag(abstract_token)
    obj = ""
    for token in abstract_pos_tag:
        if token[1].startswith("VB"):
            break
        else:
            obj = obj + token[0]+" "
    return obj


def candidate_entity_ranking(record):
    key,text,token,entities = record
    score_max = 0
    entity_score_max = ""
    for entity in entities:
        abstract = entity[1]
        score = 0
        # compare abstract object and token
        abstract_object = find_abstract_object(abstract)
        if abstract_object != "":
            if cosine_sim(abstract_object, token) < 0.1:
                continue
            else:
                score = score + cosine_sim(abstract_object, token)
        score = cosine_sim(text,abstract)
        if score > score_max:
            score_max = score
            entity_score_max = entity[0]
    if score_max != 0:
        yield key + '\t' + token + '\t' + entity_score_max

rdd = rdd.flatMap(candidate_entity_ranking)


###########################################################
#############   USELESS CODE  #############################
###########################################################


def linking(record):
    key,text,token = record
    score_max = 0
    entity_score_max = ""
    entities = search_candidate(token)
    for entity, labels in entities:
        # abstract = query_abstract(SPARQL, entity)
        abstract = query_candidate_abstract(entity)
        if abstract != None:
            score = cosine_sim(text, abstract)
            if score > score_max:
                score_max = score
                entity_score_max = entity
    if score_max != 0:
        yield key + '\t' + token + '\t' + entity_score_max

# rdd = rdd.flatMap(linking)

def Entities_Linking(record):
    _,payload = record
    key = find_key(payload)
    text = None
    tagged_tokens = None
    # res = ""
    if key != "":
        text = html2text(payload)
        tagged_tokens = nlp_preproc(text)
        mentions_types = []
        entity_result_dict = {}
        for token in tagged_tokens:
            if token not in mentions_types:
                mentions_types.append(token)
        for token in mentions_types:
            Tfidf_score_max = 0
            entity_score_max = ""
            entities = search_candidate(token[0])

            for entity,labels in entities:
                # abstract = query_abstract(SPARQL,entity)
                abstract = query_candidate_abstract(entity)
                # print(entity,labels)
                if abstract != None:
                    score = cosine_sim(text,abstract)
                    if score > Tfidf_score_max:
                        Tfidf_score_max = score
                        entity_score_max = entity

            if Tfidf_score_max != 0:
                entity_result_dict[token[0]] = entity_score_max
                # print(token)
        #        yield key + '\t' + token[0]  + '\t' + entity_score_max
        for token in tagged_tokens:
            if entity_result_dict.__contains__(token[0]):
                yield key + '\t' + token[0] + '\t' + entity_result_dict[token[0]]

# rdd = rdd.flatMap(Entities_Linking)
#################################################################
####################### USELESS CODE END ########################
#################################################################


rdd = rdd.saveAsTextFile(OUTFILE)

