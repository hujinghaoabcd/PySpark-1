from pyspark import SparkContext

sc = SparkContext("local")
data = sc.parallelize([('Amber', 22), ('Alfred', 23), ('Skye', 5), ('Albert', 12), ('Amber', 9)])
print(data.first())
print(data.collect())

data_heterogenous = sc.parallelize([
    ('Ferrari', 'fast'),
    {'Porsche': 100000},
    ['Spain', 'visited', 4504]
]).collect()
print(data_heterogenous[1]['Porsche'])
