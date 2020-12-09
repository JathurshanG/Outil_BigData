##########################TP 1 Outil Big DATA #################################
####Date de Rendu :  11/12/2020 ###############################################
##l'objectif ici est de traduire du scale en python ###########################


import sys
from pyspark import SparkContext, SparkConf

    words = sc.textFile("D:/workspace/spark/input.txt").flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)