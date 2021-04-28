# BRATH FIRST SEARCH
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

startCharacterId = 5306 # Spiderman
targetCharacterId = 2664  # Ironman

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroId = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    
    color = "WHITE"
    distance = 9999

    if (heroId == startCharacterId):
        color = "GRAY"
        distance = 0
    
    return (heroId, (connections, distance, color))

def createStartingRdd():
    inputFile = sc.textFile("C:/repositorios/Spark-Course/data/marvel/Marvel+Graph")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterId = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GRAY'):
        for connection in connections:
            newCharacterId = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterId == connection):
                hitCounter.add(1)
            
            newEntry = (newCharacterId, ([], newDistance, newColor))
            results.append(newEntry)
        
        color = 'BLACK'
    
    results.append((characterId, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)
    
    if (distance1 < distance):
        distance = distance1
    
    if (distance2 < distance):
        distance = distance2
    
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
    
    return (edges, distance, color)



# Main program
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    mapped = iterationRdd.flatMap(bfsMap)

    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) + " different direction(s)")
        break
    
    iterationRdd = mapped.reduceByKey(bfsReduce)

