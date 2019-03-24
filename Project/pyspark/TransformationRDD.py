from pyspark import SparkContext
import re
import numpy as np

sc = SparkContext("local")
data_from_file = sc.textFile('VS14MORT_MINI.txt')

def extractInformation(row):
    selected_indices = [
         2,4,5,6,7,9,10,11,12,13,14,15,16,17,18,
         19,21,22,23,24,25,27,28,29,30,32,33,34,
         36,37,38,39,40,41,42,43,44,45,46,47,48,
         49,50,51,52,53,54,55,56,58,60,61,62,63,
         64,65,66,67,68,69,70,71,72,73,74,75,76,
         77,78,79,81,82,83,84,85,87,89
    ]
    record_split = re \
        .compile(
        r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})([0-9]{2})' +
        r'([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})([0-9]{2})([0-9]{2})' +
        r'([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})([0-9]{1})([\s]{16})([0-9]{4})' +
        r'([YNU]{1})([0-9\s]{1})([BCOU]{1})([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})' +
        r'([A-Z0-9\s]{4})([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})' +
        r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})([0-9\s]{1})' +
        r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs
#数据预处理
data_from_file_conv = data_from_file.map(extractInformation)
#print(data_from_file_conv.map(lambda row: row).take(1))
#将数据集加入到内存
data_from_file_conv.cache()

#map每一行的转换
data_2014 = data_from_file_conv.map(lambda row: int(row[16]))
data_2014_2 = data_from_file_conv.map(
    lambda row: (row[16], int(row[16]))
)
#print(data_2014_2.take(10))

#filter从数据集中筛选特定的元素
data_filtered = data_from_file_conv.filter(
    lambda row: row[16] == '2014' and row[21] == ' '
)
#print(data_filtered.count())

#flatMap返回一个扁平的列表。
data_2014_flat = data_from_file_conv.flatMap(
    lambda row: (row[16], int(row[16])+1)
)
#print(data_2014_flat.take(10))

#distinct去重
distinct_gender = data_from_file_conv.map(
    lambda row: row[5]
).distinct()
#print(distinct_gender.collect())

#sample返回数据集的随机样本
fraction = 0.1
data_sample = data_from_file_conv.sample(False, fraction, 666)
#print('Original dataset: {0},sample: {1}'.format(data_from_file_conv.count(), data_sample.count()))

#leftOuterJoin左外连接
rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', 6), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2)
#print(rdd3.collect())

#join只会得到关联数值
rdd4 = rdd1.join(rdd2)
#print(rdd4.collect())

#repartion重新进行分区
#print(len(rdd1.glom().collect()))
rdd1 = rdd1.repartition(2)
#print(len(rdd1.glom().collect()))

#take返回前n行
data_first = data_from_file_conv.take(1)
#print(data_first)
data_take_sampled = data_from_file_conv.takeSample(False, 1, 667)
#print(data_take_sampled)

#collect将RDD元素返回给驱动程序

#reduce使用指定的方法减少RDD中的元素
#print(rdd1.map(lambda row: row[1]).reduce(lambda x, y: x+y))
data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)
works = data_reduce.reduce(lambda x, y: x / y)
#print(works)

data_reduce1 = sc.parallelize([1, 2, .5, .1, 5, .2], 3)
works1 = data_reduce1.reduce(lambda x, y: x / y)
#print(works1)

data_key = sc.parallelize(
    [('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4
)
#print(data_key.reduceByKey(lambda x, y: x + y).collect())


#count计算RDD中的元素数量
data_reduce.count()

print(data_key.countByKey().items())

#saveAsTextFile将RDD保存为文本文件
data_key.saveAsTextFile(
    'data_key.txt'
)

def parseInput(row):
    pattern = re.compile(r'\(\'([a-z])\', ([0-9])\)')
    row_split = pattern.split(row)
    print(row_split)
    return (row_split[1], int(row_split[2]))
data_key_reread = sc.textFile(
    'data_key.txt'
).map(parseInput)
#print(data_key_reread.collect())

data_key = sc.parallelize(
    [('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4
)
def f(x):
    print(x)
data_key.foreach(f)
