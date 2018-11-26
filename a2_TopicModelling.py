# Packages
# Pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# General packages
import sys
import collections
import os
import re
import numpy as np
import string
import json
from stop_words import get_stop_words
# Newspaper articles
import newspaper
from newspaper import Article
import time
# BeatifulSOup
from bs4 import BeautifulSoup
from bs4.element import Comment
# NLTK
import nltk
from nltk.tag import StanfordNERTagger  # NER
from nltk.stem.porter import *
# nltk.download('words')
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')

#LDA - NMF
import scipy
from gensim import corpora, models
import gensim
import pyLDAvis.gensim

import matplotlib.pyplot as plt

# Input parameters
if (sys.argv[1] == 'help'):
    print('Usage: <Input mode - WARC or ARTICLE> <If WARC: Paht to input file / If ARTICLE: Articles date y/m/d> <1 - Entities, 2-Full text> <Number of topics> <Output directory>')
input_mode = sys.argv[1].upper()
if (input_mode == 'ARTICLE'):
    date = sys.argv[2]
if (input_mode == 'WARC'):
    in_file = sys.argv[2]
rec_mode = sys.argv[3]
topic_number = sys.argv[4]
directory = sys.argv[5]

# Spark config and session
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# Functions
# Function to get only visible text in HTML - From https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text


def tag_visible(element):
    # Filter elements with following tags
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]', 'footer']:
        return False
    # Filter comments
    if isinstance(element, Comment):
        return False
    return True

# Extract text from HTML


def get_text(html):
    soup = BeautifulSoup(html, "html5lib")  # Extract HTMLContent
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

# Function to extract plain text from HTML content of the WARC file


def processWarcfile(record):
    _, payload = record
    value = ''
    # Check if the WARC block contains HTML code
    if ('<html' in payload):
        html = payload.split('<html')[1]
        value = get_text(html)
    yield value

# Natural language processing of the plain text: tokenization, removing stop words and lemmatization


def NLP(record):
    en_stop = get_stop_words('en')
    punctuation = set(string.punctuation)
    #stemmer = PorterStemmer()
    tokenized_text = nltk.word_tokenize(record)
    tokenized_text = [i for i in tokenized_text if i not in en_stop]
    tokenized_text = [i for i in tokenized_text if i not in punctuation]
    #tokenized_text = [stemmer.stem(i) for i in tokenized_text]
    if rec_mode == '1':  # If topic modelling with entities
        # Option 1 - Word tokenization
        tokenized_text = nlp.tag(tokenized_text)

    yield tokenized_text

# If rec mode == 1 - Topic modelling with entities - Function to extract Named entities


def get_entities_StanfordNER(record):
    entities = []
    for i in record:
        if (i[1] != 'O' and i[0] not in entities):
            entities.append(i[0])

    yield entities


# Option 1 - Topic modelling from CNN articles
if input_mode == 'ARTICLE':
    if not date:
        date = time.strftime("%Y/%m/%d")
    # Get CNN articles
    date_articles = []
    cnn_paper = newspaper.build('http://cnn.com', memoize_articles=False)
    for article in cnn_paper.articles:
        if str(date) in article.url:
            article = Article(article.url, keep_article_html=True)
            article.download()
            article.parse()
            date_articles.append(get_text(article.html))
    rdd = sc.parallelize(date_articles)

# Option 2 - Topic modelling from WARC file.
if input_mode == 'WARC':
    # Read warc file and split in WARC/1.0
    rdd = sc.newAPIHadoopFile(in_file,
                              "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                              "org.apache.hadoop.io.LongWritable",
                              "org.apache.hadoop.io.Text",
                              conf={"textinputformat.record.delimiter": "WARC/1.0"})

    rdd = rdd.flatMap(processWarcfile)

# Common processing for both options
# Path may change
classifier = 'stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz'
jar = 'stanford-ner/stanford-ner.jar'  # Path may change
nlp = StanfordNERTagger(classifier, jar)

rdd_result = rdd.flatMap(NLP)
if rec_mode == '1':
    rdd_result = rdd_result.flatMap(get_entities_StanfordNER)

#LDA - Gensim
# Convert RDD to dataframe
df = rdd_result.map(lambda x: (x, )).toDF(schema=['text'])

words = df.select('text').collect()
text_list = []
for i in words:
    if i[0]:
        text_list.append(i[0])

dictionary = corpora.Dictionary(text_list)
corpus = [dictionary.doc2bow(j) for j in text_list]
ldamodel = gensim.models.ldamodel.LdaModel(
    corpus, num_topics=int(topic_number), id2word=dictionary, passes=20)

#print(ldamodel.print_topics(num_topics=int(topic_number), num_words=5))

# Get and save topics results
ldatopics = ldamodel.show_topics(formatted=False)

if not os.path.exists(directory):
    os.makedirs(directory)

filepath = os.path.join(directory, 'LDA_topics.txt')
file = open(filepath, 'w+')
if input_mode == 'ARTICLE':
    file.write('Results for CNN articles of : %s\n' % date)
if input_mode == 'WARC':
    file.write('Results for WARC file : %s\n' % in_file)
for topicid, topic in ldatopics:
    top10 = []
    i = topicid
    for word, prob in topic:
        top10.append(word)
    file.write('Topic%i:%s\n' % (i, top10))

# LDA visualization
data = pyLDAvis.gensim.prepare(ldamodel, corpus, dictionary)
vispath = os.path.join(directory, 'LDA_visualization.html')
pyLDAvis.save_html(data, vispath)

# Coherence Analysis
ldatopics = [[word for word, prob in topic] for topicid, topic in ldatopics]
lda_coherence = gensim.models.coherencemodel.CoherenceModel(
    topics=ldatopics, texts=text_list, dictionary=dictionary, window_size=10).get_coherence()
lda_coherence_topic = gensim.models.coherencemodel.CoherenceModel(
    topics=ldatopics, texts=text_list, dictionary=dictionary, window_size=10).get_coherence_per_topic()

file.write('LDA Model Coherence: %s\n' % lda_coherence)
file.close()
# Generate plot with topic coherences
# Topic index
index = []
for j in range(0, int(topic_number)):
    formatting_template = 'Topic%s'
    i = formatting_template % j
    index.append(i)

assert len(lda_coherence_topic) == len(index)
n = len(lda_coherence_topic)
x = np.arange(n)
plt.bar(x, lda_coherence_topic, width=0.2, tick_label=index, align='center')
plt.xlabel('Topics')
plt.ylabel('Coherence Value')
figpath = os.path.join(directory, 'Topic_coherence_LDA')

plt.savefig(figpath, dpi=None, facecolor='w', edgecolor='w',
            orientation='portrait', papertype=None, format=None,
            transparent=False, bbox_inches=None, pad_inches=0.1,
            frameon=None)

print('The results are in the specified directory')
