# neo4jPOC
Simple Dijkstra algorithm implementation using python3 and Neo4j
#run
python3 shortestsPath.py [Neo4jPass] [StartNode] [EndNode] [Input]
#input
csv in format: 
EdgeStart EdgeEnd EdgeWeight
#example
python3 shortestsPath.py YourPassword KATOWICE KRAKOW edges.csv
