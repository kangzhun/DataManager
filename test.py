# -*- coding: utf-8 -*-

from py2neo import authenticate, Graph, NodeSelector
from pandas import DataFrame

HOST_PORT = "localhost:7474"
USER = "kangzhun"
PWD = "741953"
authenticate(HOST_PORT, USER, PWD)

movie_graph = Graph("http://localhost:7474/db/data/")
print DataFrame(movie_graph.data("MATCH (a:Person) RETURN a.name, a.born LIMIT 4"))

print movie_graph.run("MATCH (a) WHERE a.name={x} RETURN a.born", x="Keanu Reeves").evaluate()

selector = NodeSelector(movie_graph)
selected = selector.select(name="Keanu Reeves")
print list(selected)

selected = selector.select().where("_.name =~ 'J.*'", "1960 <= _.born < 1970")
print list(selected)
