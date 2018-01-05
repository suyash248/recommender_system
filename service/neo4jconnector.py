from py2neo import Graph
from etc.conf.settings import neo4j_config

neo4j_con = Graph(**neo4j_config)