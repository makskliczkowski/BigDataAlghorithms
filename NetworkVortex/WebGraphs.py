from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark import SparkConf
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark


# ----------------DIRECTED----------------------
def directedInOutMerger(x):
    # this makes a list of tuples for out and in, as we can se the first
    # element is out and the second is in if we treat directed graph
    # the we will add them with reducers and we will see the numbers
    temp = x.split("\t")
    # print(temp[0])
    temp2 = [(temp[0], [1, 0]), (temp[-1], [0, 1])]  # out from temp[0], in from temp[1]
    # print(temp2)
    return temp2


def directedCountInAndOut(file):
    # entry point of spark cluster
    sc = SparkContext("local", "DirectedGraphCounter")
    # read file into RDD
    lines = sc.textFile(file).filter(lambda x: '#' not in x)
    # collect all the data
    howManyWereForSingle = lines.count()
    print(howManyWereForSingle)
    # change to lists of lists
    mp = lines.flatMap(directedInOutMerger)
    # reduce by key
    rd = mp.reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1]])
    # a = rd.reduce(lambda a, b: [a[0] + b[0], a[1] + b[1]])
    ls = rd.collect()
    for line in ls:
        print(line)
    print('\nThe average is:' + str(howManyWereForSingle / len(ls)))
    print('\nWe know that for a directed graph the sum must be the same for in an for out\n')


# ----------------------------UNDIRECTED
def undirectedInOutMerger(x):
    # this makes a list of tuples for out and in, as we can se the first
    # element is out and the second is in if we treat directed graph
    # the we will add them with reducers and we will see the numbers
    temp = x.split("\t")
    # print(temp[0])
    # a = temp[0]
    # b = temp[1]
    # to keep the query
    # if int(b) < int(a):
    #    a = b
    #    b = temp[0]
    # temp_tuple = (a, b)
    # write edges so we have a good order
    temp2 = [(temp[0], [temp[-1]]), (temp[-1], [temp[0]])]  # out from temp[0], in from temp[1]
    # print(temp2)
    return temp2


def takeGroupedAndGroupByEdges(x):
    # this function allows us to create a tuple of edge, and list of edges
    # for given vortex, so there will be two after all
    # and in next function we will check how many
    # vortices match
    ls = []
    dimv1 = len(x[-1])
    # we give it's len

    # now we can only
    for i in x[-1]:
        g = [0, 0]
        if int(i) > int(x[0]):
            index = 0
            tuple_to_list = (x[0], i)
            g[index] = dimv1
            ls.append((tuple_to_list, [g, x[-1]]))
        else:
            index = 1
            tuple_to_list = (i, x[0])
            g[index] = dimv1
            ls.append((tuple_to_list, [g, x[-1]]))

    return ls


def checkIfNeiOfNeiAreInNei(a, b):
    # here we check, using reducer, that if a given vortex a has N_a and b has N_b
    # how many of them match so we can calculate local clusters
    # this is almost last step
    # we know that there will two of such tuples

    # print('--------------\n', a[0], '\n', b[0] ,'\n')
    num_the_same = len(list(set(a[-1]).intersection(set(b[-1]))))
    g = [int(a[0][0]) + int(b[0][0]), int(a[0][1]) + int(b[0][1])]
    return [g, num_the_same]


def flatMapToGetVertices(x):
    clust1 = 0.0 if x[-1][0][0] == 1 else x[-1][-1] * 2.0 / (x[-1][0][0] * x[-1][0][0] - x[-1][0][0])
    clust2 = 0.0 if x[-1][0][1] == 1 else x[-1][-1] * 2.0 / (x[-1][0][1] * x[-1][0][1] - x[-1][0][1])

    return [(x[0][0], [clust1, x[-1][0][0], 1.0]),
            (x[0][1], [clust2, x[-1][0][1], 1.0])
            ]  # [(v1,nthesame,dimv1),(v2,nthesame,dimv2)]


def reduceToGetLastInfo(a, b):
    return [a[0] + b[0], a[1], 1]


def averageCal(a, b):
    # print(a,'\n',b,'\n')
    return ('', [a[-1][0] + b[-1][0], a[-1][1] + b[-1][1], a[-1][2] + b[-1][2]])


def undirectedCountInAndOut(file):
    # entry point of spark cluster
    conf = pyspark.SparkConf()
    conf.set('spark.local.dir', 'RDD/')

    # sc = SparkContext("local", "DirectedGraphCounter")
    sc = pyspark.SparkContext(conf=conf)
    # read file into RDD

    lines = (sc
             .textFile(file)  # read from file
             .flatMap(undirectedInOutMerger)
             .reduceByKey(lambda a, b: list(set(a + b)))  # create a list of edges for given val (val,L[edge])
             .flatMap(takeGroupedAndGroupByEdges)  # for each edge make (edge, L[edges])
             .reduceByKey(checkIfNeiOfNeiAreInNei)  # for each check neighbours
             .flatMap(flatMapToGetVertices)
             .reduceByKey(lambda a, b: [a[0] + b[0], a[1], 1.0])
             )

    # ls = lines.collect()
    # for line in ls:
    #    print(line)

    for_average = lines.reduce(averageCal)
    average = for_average[-1][0] / for_average[-1][2]
    print('Average clustering coefficient is: ' + str(average / 2) + '\n')
    lines.saveAsTextFile('saved_avClustCoeff_' + str(average / 2) + '.txt')


try:

    undirectedCountInAndOut('web-Stanford.txt')
except ...:
    spark = SparkSession.builder.getOrCreate()
    spark.catalog.clearCache()
