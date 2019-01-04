from extensionhtml2text import html2text
from nlp_preproc_spark import nlp_preproc
from elasticsearch import search

import sys
import nltk
import unidecode
import re
import numpy as np

# import time
from sparql import query_abstract

# from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
#from sklearn.feature_extraction.text import CountVectorizer
#from sklearn.feature_extraction.text import TfidfTransformer

KEYNAME = "WARC-TREC-ID"

def find_key(payload):
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            return key;
    return '';

def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line

vectorizer = TfidfVectorizer()
def cosine_sim(text1, text2):
        tfidf = vectorizer.fit_transform([text1, text2])
        return ((tfidf * tfidf.T).A)[0,1]


def compute_jaccard_index(set1, set2):
    if len(set1) == 0 or len(set2) == 0:
        return 0
    ins = len(set1.intersection(set2))
    uni = float(len(set1.union(set2)))
    if uni != 0:
        return ins / uni
    return 0

def clean_word(word): return re.sub(r'[^a-z]+', '', unidecode.unidecode(word).lower())

def salient_Jaccard(title, abstract):
    words1 = {clean_word(word) for word in nltk.word_tokenize(title) if len(clean_word(word)) > 0}
    words2 = {clean_word(word) for word in nltk.word_tokenize(abstract) if len(clean_word(word)) > 0}
    jaccard_index = compute_jaccard_index(words1, words2)
    return  jaccard_index

entity_dict = {}
def search_candidate(token):
    entities = None
    if entity_dict.__contains__(token):
        entities = entity_dict[token]
    else:
        entities = search(ELASTICSEARCH,token).items()
        entity_dict[token] = entities
    return entities

abstract_dict = {}
def query_candidate_abstract(entity):
    abstract = ""
    if abstract_dict.__contains__(entity):
        abstract = abstract_dict[entity]
    else:
        abstract = query_abstract(SPARQL, entity)
        abstract_dict[entity] = abstract
    return abstract


if __name__ == '__main__':
    import sys
    try:
        _, INPUT, ELASTICSEARCH,SPARQL = sys.argv
    except Exception as e:
        print('Usage: python starter-code.py INPUT ELASTICSEARCH SPARQL')
        sys.exit(0)


    with open(INPUT, errors='ignore') as fo:
        ## split input into records using "WARC/1.0"
    #    n = 0
        for record in split_records(fo):
            key = find_key(record);
    #        n += 1
            if key != '':
                #print(key)
                contextall = html2text(record)
                text = contextall[0]
                title =  contextall[1]
                #print(title)
                #print(text)
                if title == "" or text == "":
                    continue;
                tagged_tokens = nlp_preproc(text)
                mentions_types = []
                entity_result_dict = {}
                result_entities = []
                for token in tagged_tokens:
                    if token not in mentions_types:
                        mentions_types.append(token)

                for token in mentions_types:
               # for token in tagged_tokens:
                    Tfidf_score_max = 0
                    entity_score_max = ""
                    entities = search_candidate(token[0])

                    for entity,labels in entities:
                        abstract = query_candidate_abstract(entity)
                        # print(entity,labels)
                        if abstract != None:
                            score = cosine_sim(text,abstract)
                            if score > Tfidf_score_max:
                                Tfidf_score_max = score
                                entity_score_max = entity

                    if Tfidf_score_max != 0:
                        # print(token)
                        # print(key + "\t" + token[0] + "\t"+ entity_score_max)
                        entity_result_dict[token[0]] = entity_score_max
                        result_entities.append([token[0],entity_score_max])

                #for token in tagged_tokens:
                    #if entity_result_dict.__contains__(token[0]):
                        #print( key + '\t' + token[0] + '\t' + entity_result_dict[token[0]])

                salient_score_max = 0
                salient_entity = None
                for s_entity in result_entities:
                    s_abstract = query_candidate_abstract(s_entity[1])
                    if s_abstract != None:
                        salient_score =  salient_Jaccard(title, s_abstract)
                        if  salient_score > salient_score_max:
                            salient_score_max  = salient_score
                            salient_entity =  s_entity[1]
                if salient_entity != None:
                    print(key + " salient entity: ",s_entity[0]+" "+ salient_entity)
            #if n > 10:
            #    break



