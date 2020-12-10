##########################TP 1 Outil Big DATA #################################
####Date de Rendu :  11/12/2020 ###############################################
##l'objectif ici est de traduire du scale en python ###########################

#Initalisation de spark, au préalable télécharger findSparks avec pip

import findspark 
findspark.init("C:\Spark")

#entrypoint des API Spark
from pyspark import SparkContext, SparkConf

conf =SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)

word=sc.textFile("sample.txt")
#exportation des données choisit soit sample.txt
count=word.flatMap(lambda line: line.split(" "))
#dans cette étape on considère comme donnée chaque mots, l'éspace étant
count = count.map(lambda word: (word, 1))
#Creation d'un tuple permettant de compte chaque mots
count=count.reduceByKey(lambda a,b:a +b)
#on associe a chaque le nombre de fois où apparaît le mots
print(count.collect)
#afficher les mots