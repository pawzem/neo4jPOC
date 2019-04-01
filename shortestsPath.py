from neo4j import GraphDatabase
import csv
import queue
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: Any = field(compare=False)


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", sys.argv[1]))


def add_edge(tx, number, destination, weight):
    tx.run("MERGE (e:Edge {number: $number}) "
           "MERGE (d:Edge {number: $destination}) "
           "MERGE (e)-[c:CONNECTS {weight: $weight}]->(d)",
           number=number, destination=destination, weight=weight)


def print_edges(tx, number):
    for record in tx.run("MATCH (e:Edge)-[:CONNECTS]->(connection) WHERE e.number = $number "
                         "RETURN connection ORDER BY connection.number", number=number):
        print(record["connection"])


def get_edges(tx, number):
    edges = []
    for record in tx.run("MATCH e =(edge { number: $number })-[*..1]->()"
                         "RETURN e", number=number):
        edges.append(record["e"].relationships[0])
    return edges


def purge(tx):
    tx.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")


def prepare_queue(root, distances):
    distances[root] = 0
    q = queue.PriorityQueue()
    for v, d in distances.items():
        q.put(PrioritizedItem(d, v))
    return q


def dijkstra(root, destination, distances):
    q = prepare_queue(root, distances)
    while not q.empty():
        current = q.get()
        for edge in session.write_transaction(get_edges, current.item):
            if distances[edge.end_node['number']] > (distances[current.item] + edge['weight']):
                distances[edge.end_node['number']] = (distances[current.item] + edge['weight'])
    return distances[destination]


def read_graph(input_file):
    distances = {}
    csv_reader = open(input_file, 'r')
    edges_reader = csv.reader(csv_reader, delimiter=' ')
    for row in edges_reader:
        print("reading edge: " + ', '.join(row))
        distances[row[0]] = sys.maxsize
        distances[row[1]] = sys.maxsize
        session.write_transaction(add_edge, row[0], row[1], int(row[2]))
    return distances


with driver.session() as session:
    root_city = sys.argv[2]
    destination_city = sys.argv[3]
    file = sys.argv[4]

    session.write_transaction(purge)
    distances_to = read_graph(file)

    print(dijkstra(root_city, destination_city, distances_to))

