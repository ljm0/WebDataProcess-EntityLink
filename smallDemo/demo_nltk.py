import nltk
from nltk.tag import StanfordNERTagger  # NER
# from nltk.corpus import treebank
# #Tokenize and tag some text
# sentence = """At eight o'clock on Thursday morning... Arthur didn't feel very good."""
# tokens = nltk.word_tokenize(sentence)
# print("tokenize: ", tokens)
# tagged = nltk.pos_tag(tokens)
# print("tag: ", tagged[0:6])
# #Identify named entities
# entities = nltk.chunk.ne_chunk(tagged)
# print("Identify named entities: ", entities)
# #Display a parse tree:
# t = treebank.parsed_sents('wsj_0001.mrg')[0]
# t.draw()

def NLP_NER(record):
    #sent_text = nltk.sent_tokenize(record)
    tokenized_text = nltk.word_tokenize(record)
    #FIXME TypeError: a bytes-like object is required, not 'str'
    # tokenized_text = [x.encode('utf-8') for x in tokenized_text]
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

record_test = 'xmlns "http //www.w3.org/1999/xhtml" xml lang "en" lang "en"> rss atom rdf Photos aggregator dynamic content Search Add album/Contact us News Reviews shaggyshoo has added a photo to the pool annecy. france. France image Pool 2012-02-10 16 22 52 February 10th, 2012 Tags cloud 2008 amateur photographer baby blue car cat con ct ds el est flickr hot ice image la lady man me men mer nb ol one people photo photos photos pictures amateur photographer pictures port q ran red riot Tunis Tunisia up us vie xt WP Cumulus Flash tag cloud by Roy Tanck and Luke Morton requires Flash Player 9 or better. Twits from \'photobabble\' No public Twitter messages. Based on Ocular Professor Powered by WordPress'

result = NLP_NER(record_test)
for result in NLP_NER(record_test):
    print(result)
