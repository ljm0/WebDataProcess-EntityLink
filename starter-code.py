from html2text import html2text
from nlp_preproc_spark import nlp_preproc
from elasticsearch import search

#from datetime import datetime
# import time
from sparql import query_abstract
import nltk
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

# dictionary for elasticsearch: {token:[candidate Freebase entity IDs]}
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


def find_abstract_object(abstract):
    abstract_token = nltk.word_tokenize(abstract)
    abstract_pos_tag = nltk.pos_tag(abstract_token)
    obj = ""
    # noun phrase before the first verb is the object of the abstract
    for token in abstract_pos_tag:
        if token[1].startswith("VB"):
            break
        else:
            obj = obj + token[0]+" "
    return obj



if __name__ == '__main__':
    import sys
    try:
        _, INPUT, ELASTICSEARCH,SPARQL = sys.argv
    except Exception as e:
        print('Usage: python starter-code.py INPUT ELASTICSEARCH SPARQL')
        sys.exit(0)


    with open(INPUT, errors='ignore') as fo:
        ## split input into records using "WARC/1.0"
        # num = 0
        # total = 0
        for record in split_records(fo):
            key = find_key(record);
            if key != '':
                #print(key)
                # begin = datetime.now()
                text = html2text(record)
                tagged_tokens = nlp_preproc(text)
                mentions_types = []
                entity_result_dict = {}
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
                        score = 0
                        if abstract != None:
                            # compare abstract object and token
                            abstract_object = find_abstract_object(abstract)
                            if abstract_object != "":
                                if cosine_sim(abstract_object, token[0]) < 0.1:
                                    continue
                                else:
                                    score = score + cosine_sim(abstract_object, token[0])
                            # compare abstract and text
                            score = score + cosine_sim(text,abstract)
                            # find the max score entity
                            if score > Tfidf_score_max:
                                Tfidf_score_max = score
                                entity_score_max = entity

                    if Tfidf_score_max != 0:
                        # print(token)
                        # print(key + "\t" + token[0] + "\t"+ entity_score_max)
                        entity_result_dict[token[0]] = entity_score_max
                # ouput result
                for token in tagged_tokens:
                    if entity_result_dict.__contains__(token[0]):
                        print( key + '\t' + token[0] + '\t' + entity_result_dict[token[0]])
                # calculate runtime
                # end = datetime.now()
                # if num < 10:
                #     total = total + (end - begin).seconds
                # num = num +1
       # print(total/10)




