from pyspark import SparkContext, SparkConf
import sys
import collections
import os
from subprocess import call, Popen
import re
import requests
import json
import math
import numpy
import urllib

from bs4 import BeautifulSoup
from bs4.element import Comment

import nltk
from nltk.tag import StanfordNERTagger  # NER

nltk.download('words')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

import scipy
from sklearn.feature_extraction.text import TfidfVectorizer


sc = SparkContext.getOrCreate()

# Check input parameters
#if len(sys.argv) < 5 or len(sys.argv) > 5:
#    print('Usage - <Warc_key> <Input_file> <output_file>')
#else:
record_attribute = sys.argv[1]
in_file = sys.argv[2]
out_file = sys.argv[3]
search_url = sys.argv[4]


# Read warc file and split in WARC/1.0
rdd = sc.newAPIHadoopFile(in_file,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})


# Process WARC file -- Convert RDD to tuples (key (WARC-Record-ID), value (Text from HTML content))
# 1. Get the key
# 2. Get HTML content to each page (and associated it to the corresponding key)
# 3. Get Text from the HTML content

# Function to get only visible text in HTML - From https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text
def tag_visible(element):
    # Filter elements with following tags
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    # Filter comments
    if isinstance(element, Comment):
        return False
    return True


def get_text(html, flag):
    soup = BeautifulSoup(html, "html5lib")  # Extract HTMLContent
    if flag == 1:
        value = soup.find(
            "span", {"property": "dbo:abstract", "xml:lang": "en"})
        if value is not None:
            value = value.getText()
        else:
            value = ''
    else:
        plain_text = soup.findAll(text=True)  # Get plain text
        value = filter(tag_visible, plain_text)  # Get only visible text
        # Format the text
        value = " ".join(value)
        # Replace special unicode characters
        value = re.sub(r'[^\x00-\x7F]+', ' ', value)
        # Replace special characters
        value = re.sub(r'[(?<=\{)(:*?)(?=\})]', ' ', value)
        value = ' '.join(value.split())

    return value

# Function to extract the WARC key and plain text from HTML content of the WARC file


def processWarcfile(record):
    _, payload = record
    key = None
    value = None
    for line in payload.splitlines():
        if line.startswith(record_attribute):
            key = line.split(': ')[1]  # Save <WARC-Key>
            break
    # Check if the WARC block contains HTML code
    if key and ('<html' in payload):
        html = payload.split('<html')[1]
        value = get_text(html, 0)
        yield (key, value)


rdd_pairs = rdd.flatMap(processWarcfile)  # RDD with tuples (key, text)
# Save plain text before transform it for disambiguation
rdd_disambiguation = rdd_pairs
# print(rdd_pairs.collect())
# rdd_print = rdd_pairs.saveAsTextFile(out_file)

#NLP - NER
# 1. Tokenization
# 2. NER - StanfordNER


def NLP_NER(record):
    #sent_text = nltk.sent_tokenize(record)
    tokenized_text = nltk.word_tokenize(record)
    #FIXME TypeError: a bytes-like object is required, not 'str'
    #tokenized_text = [x.encode('utf-8') for x in tokenized_text]
    tag_text = nltk.pos_tag(tokenized_text)

    # StanfordNER
    ner_text_NER = nlp.tag(tokenized_text)  # Option 1 - Word tokenization
    # ner_text = [nlp.tag(s.split()) for s in sent_text] #Option 2 - Sentece tokenization

    yield ner_text_NER


# StanfordNERTagger - Files needed
# Path may change
classifier = '/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/classifiers/english.all.3class.distsim.crf.ser.gz'
# Path may change
jar = '/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/stanford-ner.jar'
nlp = StanfordNERTagger(classifier, jar)

# RDD tuples (key, tuple(word, label))
rdd_ner = rdd_pairs.flatMapValues(NLP_NER)

# Entity extraction
# Extract Name Entities from result - Function to get recognized entities from Stanford NER
def get_entities_StanfordNER(record):
    entities = []
    for i in record:
        if (i[1] != 'O' and i[0] not in entities):
            entities.append(i[0])

    yield entities

# Extract Muliterm Name Entities from result - Function to get recognized entities from Stanford NER


def get_entities_StanfordNER_multiterm(record):
    entities = []
    last_tag = None
    for i in record:
        if (i[1] != 'O' and i[0] not in entities):
            if i[1] == last_tag:
                if i[0] not in entities[len(entities)-1]:
                    entities[len(entities) -
                             1] = entities[len(entities)-1]+' '+i[0]
            else:
                entities.append(i[0])
        last_tag = i[1]

    yield entities


rdd_ner_entities = rdd_ner.flatMapValues(
    get_entities_StanfordNER)  # RDD tuples (key, entities)
#print(rdd_ner_entities.collect())
#rdd_print = rdd_ner_entities.saveAsTextFile(out_file)

# Link entities to KB
ELASTICSEARCH_URL = 'http://' + search_url + '/freebase/label/_search' % search_url
print("ELASTICSEARCH_URL:",ELASTICSEARCH_URL)
print("search_url:",search_url)


# Get IDs, label and score from ELASTICSEARCH for each entity
def get_elasticsearch(record):
    tuples = []
    for i in record:
        query = i
        response = requests.get(ELASTICSEARCH_URL, params={
                                'q': query, 'size': 1000})  # Query all the entities
        result = {}
        if response:
            response = response.json()
            for hit in response.get('hits', {}).get('hits', []):
                freebase_id = hit.get('_source', {}).get('resource')
                label = hit.get('_source', {}).get('label')
                score = hit.get('_score', 0)

                if result.get(freebase_id) == None:  # Check duplicate id
                    # If freebase_id is not in the dict, add all the extract info from JSON
                    result[freebase_id] = (
                        {'label': label, 'score': score, 'facts': 0, 'match': 0, 'text': '', 'similarity': 0})
                else:
                    score_1 = max(result[freebase_id]['score'], score)
                    # If freebase_id is in the dict, update the score but not create a new entry
                    result[freebase_id]['score'] = score_1
        # Return entity with its associated dictionary with the info from elastic search query
        tuples.append([i, result])
    yield tuples


# RDD (key, [entity, dict{freebase_id: {score,  label}}])
rdd_labels = rdd_ner_entities.flatMapValues(get_elasticsearch)
#print(rdd_labels.collect())
rdd_print = rdd_labels.saveAsTextFile(out_file)

# # Link IDs to motherKB
# TRIDENT_URL = 'http://10.141.0.125:9001/sparql'  # May change
# prefixes = """
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX fbase: <http://rdf.freebase.com/ns/>
# """
# same_as_template = prefixes + """
# SELECT DISTINCT ?same WHERE {
#     ?s owl:sameAs %s .
#     { ?s owl:sameAs ?same .} UNION { ?same owl:sameAs ?s .}
# }
# """
# po_template = prefixes + """
# SELECT DISTINCT * WHERE {
#     %s ?p ?o.
# }
# """
# # Get number of facts from KB for each entity. Calculate the 'match' from score and facts as stated in https://github.com/bennokr/wdps2017/query.py


# def get_motherKB(record):
#     tuples = []
#     for i in record:
#         entity = i[0]
#         for key in i[1]:
#             response = requests.post(
#                 TRIDENT_URL, data={'print': False, 'query': po_template % key})
#             if response:
#                 response = response.json()
#                 n = int(response.get('stats', {}).get('nresults', 0))
#                 i[1][key]['facts'] = n
#                 if n != 0:
#                     i[1][key]['match'] = math.log(n) * i[1][key]['score']
#                 else:
#                     i[1][key]['match'] = 0
#         tuples.append([entity, i[1]])
#     yield tuples


# rdd_ids = rdd_labels.flatMapValues(get_motherKB)


# # Get 5 best matches from the complete set of results and perform entity disambiguation with the 5 best results
# def get_bestmatches(record):
#     best_matches = {}
#     tuples = []
#     links = []
#     for i in record:
#         entity = i[0]
#         best_matches = dict(
#             sorted(i[1].items(), key=lambda x: (x[1]['match']), reverse=True)[:5])
#         for key in best_matches:
#             response = requests.post(
#                 TRIDENT_URL, data={'print': True, 'query': same_as_template % key})
#             if response:
#                 response = response.json()
#                 for binding in response.get('results', {}).get('bindings', []):
#                     link = binding.get('same', {}).get('value', None)
#                     if link.startswith('http://dbpedia.org'):
#                         html = urllib.urlopen(link)
#                         if html.getcode() == 200:
#                             html = html.read()
#                             link_text = get_text(html, 1)
#                             best_matches[key]['text'] = link_text
#         tuples.append([entity, best_matches])
#     yield tuples


# rdd_ids_dis = rdd_ids.flatMapValues(get_bestmatches)


# # Choose the best result by calculating cosine similarity between the extracted text and the dbpedia text for the entity
# def cos_similarity(record):
#     tuples = []
#     vect = TfidfVectorizer(min_df=1)
#     for i in record[0]:
#         entity = i[0]
#         for key in i[1]:
#             if i[1][key]['text'] != '':
#                 text = i[1][key]['text']
#                 tfidf = vect.fit_transform([text, record[1]])
#                 pairwise_similarity = tfidf * tfidf.T
#                 i[1][key]['similarity'] = pairwise_similarity[0, 1]
#         result = dict(sorted(i[1].items(), key=lambda x: (
#             x[1]['similarity']), reverse=True)[:1])
#         tuples.append([entity, result])
#     yield tuples


# rdd_result = rdd_ids_dis.join(rdd_disambiguation)
# rdd_result = rdd_result.flatMapValues(cos_similarity)

# # Write the output to a file


# def get_output(record):
#     s = ''
#     if record[1]:
#         for i in record[1]:
#             for key in i[1]:
#                 key = key.split(':')[1]
#                 key = key.replace(".", "/")
#                 s += record[0]+"\t\t\t"+i[0]+"\t\t\t/"+key+"\n"
#     return s


# result = rdd_result.coalesce(1).map(get_output)
# #FIXME the output.tsv is a folder? yes, the ture output is inside the floder
# result.saveAsTextFile('output_floder')
# result.saveAsTextFile(out_file)
# print('The output is the file part-00000 in the directory output.tsv')
