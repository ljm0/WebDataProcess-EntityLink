### nlp_preproc.py
import nltk
# from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
# from nltk.stem import PorterStemmer
# from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
# from nltk.tag import StanfordNERTagger
import os
# from nltk.chunk import conlltags2tree
from nltk.tree import Tree
nltk.data.path.append(os.environ.get('PWD'))
nltk.download('punkt')
nltk.download('stopwords')
# nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')

# Parse named entities from tree
def structure_ne(ne_tree):
    ne = []
    for subtree in ne_tree:
        # If subtree is a noun chunk, i.e. NE != "O"
        if type(subtree) == Tree:
            ne_label = subtree.label()
            ne_string = " ".join([token for token, pos in subtree.leaves()])
            ne.append((ne_string, ne_label))
    return ne

#NLP by nltk and stanford NER
def nlp_preproc(text):
    ### Tokenization
    tokens = word_tokenize(text);

    ### Stemming & Lemmatization
    # stemmer = PorterStemmer()
    # stemmed_tokens = [stemmer.stem(s) for s in tokens]
    # lemmatizer = WordNetLemmatizer()
    # lemmatized_tokens = [lemmatizer.lemmatize(s) for s in tokens]

    ### Stop word removal
    clean_tokens = tokens[:]
    # clean_tokens = lemmatized_tokens[:]
    # clean_tokens = stemmed_tokens[:]
    sr = stopwords.words('english')
    for token in tokens:
        if token in sr:
            clean_tokens.remove(token);

    ### POS Tagging
    tagged_tokens = nltk.pos_tag(clean_tokens);
    ### NER
    ner_tagged_tokens =structure_ne(nltk.ne_chunk(tagged_tokens));
    ### NER
    # english.all.3class.distsim.crf.ser.gz
    # english.conll.4class.distsim.crf.ser.gz
    # english.muc.7class.distsim.crf.ser.gz
    # classifier = "./NER/stanford-ner-2018-10-16/classifiers/english.conll.4class.distsim.crf.ser.gz"
    # jar = "./NER/stanford-ner-2018-10-16/stanford-ner.jar"
    # classifier = "/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/classifiers/english.conll.4class.distsim.crf.ser.gz"
    # jar = "/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/stanford-ner.jar"
    # return tagged_tokens;
    # return ner_tagged_tokens;

    # st = StanfordNERTagger(classifier, jar);
    # ner_tokens =st.tag(clean_tokens) # [st.tag(line) for line in text.splitlines()];

    # ner_tagged_tokens = []
    # for token in ner_tokens:
    #     if token[1] != 'O':
    #         ner_tagged_tokens.append(token)

    return ner_tagged_tokens


