### nlp_preproc.py
import nltk
# from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
# from nltk.stem import PorterStemmer
# from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tag import StanfordNERTagger


nltk.download('punkt')
nltk.download('stopwords')
# nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')


#NLP by NLTK and stanford NER
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
    # tagged_tokens = nltk.pos_tag(clean_tokens);

    ### NER
    # english.all.3class.distsim.crf.ser.gz
    # english.conll.4class.distsim.crf.ser.gz
    # english.muc.7class.distsim.crf.ser.gz
    # classifier = "./VENV/venv/lib/stanford-ner-2018-10-16/classifiers/english.conll.4class.distsim.crf.ser.gz"
    # jar = "./VENV/venv/lib/stanford-ner-2018-10-16/stanford-ner.jar"
    classifier = "/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/classifiers/english.conll.4class.distsim.crf.ser.gz"
    jar = "/home/wdps1811/scratch/lib/stanford-ner-2018-10-16/stanford-ner.jar"
    # return tagged_tokens;
    # return ner_tagged_tokens;

    st = StanfordNERTagger(classifier, jar);
    ner_tokens =st.tag(clean_tokens) # [st.tag(line) for line in text.splitlines()];

    ner_tagged_tokens = []
    for token in ner_tokens:
        if token[1] != 'O':
            ner_tagged_tokens.append(token)

    return ner_tagged_tokens


