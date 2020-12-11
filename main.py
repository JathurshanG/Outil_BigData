# =============================================================================
# Matière : Outil Big Data
# Date de rendu : 12/12/2020
# TP concernant Le word Count
# Groupe 15
# =============================================================================

import findspark

findspark.init("C:\Spark")

#entrypoint des API Spark
from pyspark import SparkContext, SparkConf

conf =SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)

word=sc.textFile("sample.txt")
print(word.collect())

count=word.flatMap(lambda line: line.split(" "))
#dans cette étape on considère comme donnée chaque mots, l'éspace étant
count1 = count.map(lambda word: (word, 1))
#Creation d'un tuple permettant de compte chaque mots
count2 = count1.reduceByKey(lambda a,b:a +b)
#on associe a chaque le nombre de fois où apparaît le mots

for x in count2.collect():
    print(x)
#Récupération du tuple sous forme de fichier texte
count2.coalesce(1).saveAsTextFile("countWord.txt")
