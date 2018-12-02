# Initialize Spark
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')

if not 'sc' in globals():  # This 'trick' makes sure the SparkContext sc is initialized exactly once
    # Spark will use all cores (*) available
    conf = SparkConf().setMaster('local[*]')
    sc = SparkContext(conf=conf)

words_list = ['Dog', 'Cat', 'Rabbit', 'Hare',
              'Deer', 'Gull', 'Woodpecker', 'Mole']
words_rdd = sc.parallelize(words_list)
print(words_rdd.count())


def make_plural(word):
    return word + 's'
# Let's see if it works
print(make_plural('cat'))
