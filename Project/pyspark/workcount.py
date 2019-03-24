from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster('local').setAppName("wordCountPerDoc")
sc = SparkContext.getOrCreate(conf)
doc = sc.parallelize([['a', 'b', 'c'], ['b', 'd', 'd']])
words = doc.flatMap(lambda x: x).distinct().collect()
print(words)
word_dict = {w: i for w, i in zip(words, range(len(words)))}
print(word_dict)
word_dict_b = sc.broadcast(word_dict)
print(word_dict_b)
def wordCountPerDoc(d):
    dict = {}
    wd = word_dict_b.value
    for w in d:
        if wd[w] in dict:
            dict[wd[w]] += 1
        else:
            dict[wd[w]] = 1
    return dict

print(doc.map(wordCountPerDoc).collect())
print("successful")
