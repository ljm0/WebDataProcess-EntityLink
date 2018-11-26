import nltk
from nltk.corpus import treebank
#Tokenize and tag some text
sentence = """At eight o'clock on Thursday morning... Arthur didn't feel very good."""
tokens = nltk.word_tokenize(sentence)
print("tokenize: ", tokens)
tagged = nltk.pos_tag(tokens)
print("tag: ", tagged[0:6])
#Identify named entities
entities = nltk.chunk.ne_chunk(tagged)
print("Identify named entities: ", entities)
#Display a parse tree:
t = treebank.parsed_sents('wsj_0001.mrg')[0]
t.draw()
