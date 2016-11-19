"""
What does this module do?  
Does it do things?

"""

__all__ = []
__version__ = '0.1'
__author__ = 'Chris Fauerbach'
__email__ = 'chris@fauie.com'

from py2neo import authenticate, Graph, Node, Relationship
from config import Config, IndexPattern

PATTERN_TYPE = "pattern"
INDEX_TYPE = "elastic_index"
DOC_TYPE_TYPE = "doc_type"
DOC_PROPERTY_TYPE = "doc_property"

class Neo4J():
    def __init__(self, config):
        """
        :type config: Config
        """
        pass
        self.neo_host = config.neo4j_host
        self.neo_port = config.neo4j_port
        self.neo_username = config.neo4j_user
        self.neo_password = config.neo4j_password
        self.graph = self._get_graph()

    def _get_graph(self):
        graph_url = "http://{}:{}/db/data".format(self.neo_host, self.neo_port)

        neograph  = None

        if self.neo_username and self.neo_password:
            neograph = Graph(graph_url, user=self.neo_username, password=self.neo_password, bolt=False)
        else:
            neograph = Graph(graph_url, bolt=False)

        return neograph



    def persist_pattern(self, index_pattern ):
        """
        :type  index_pattern: IndexPattern
        """
        name =  index_pattern.name
        regex = index_pattern.regex

        existing_node = self.graph.find_one(PATTERN_TYPE, property_key="name", property_value=name) #type: Node

        if not existing_node:
            tx = self.graph.begin()
            new_node = Node(PATTERN_TYPE, name = name, regex = regex)
            tx.create(new_node)
            tx.commit()
            existing_node = new_node
        return existing_node

    def persist_index(self, idx_name):
        existing_node = self.graph.find_one(INDEX_TYPE, property_key="name", property_value=idx_name) #type: Node
        if not existing_node:
            tx = self.graph.begin()
            new_node = Node(INDEX_TYPE, name = idx_name)
            tx.create(new_node)
            tx.commit()
            existing_node = new_node
        return existing_node

    def persist_property(self, _property):
        existing_node = self.graph.find_one(DOC_PROPERTY_TYPE, property_key="name", property_value=_property) #type: Node
        if not existing_node:
            tx = self.graph.begin()
            new_node = Node(DOC_PROPERTY_TYPE, name =_property)
            tx.create(new_node)
            tx.commit()
            existing_node = new_node
        return existing_node

    def persist_type(self, _type):
        existing_node = self.graph.find_one(DOC_TYPE_TYPE, property_key="name", property_value=_type) #type: Node
        if not existing_node:
            tx = self.graph.begin()
            new_node = Node(DOC_TYPE_TYPE, name =_type)
            tx.create(new_node)
            tx.commit()
            existing_node = new_node
        return existing_node

    def persist_type_and_index(self, idx_name, _type):
        idx_node = self.persist_index(idx_name)
        _type_node = self.persist_type(_type)
        tx = self.graph.begin()
        relationship = Relationship(idx_node, "HAS_TYPE", _type_node)
        tx.create(relationship)
        tx.commit()

    def persist_pattern_and_type(self, index_pattern, _type):

        pattern_node = self.persist_pattern(index_pattern)
        _type_node = self.persist_type(_type)
        tx = self.graph.begin()
        relationship = Relationship(pattern_node, "RELATED_TO_TYPE", _type_node)
        tx.create(relationship)
        tx.commit()

    def persist_pattern_and_index(self, index_pattern, idx_name ):
        """
        :type  index_pattern: IndexPattern
        """
        pattern_node = self.persist_pattern(index_pattern)
        idx_node = self.persist_index(idx_name)

        tx = self.graph.begin()
        relationship = Relationship(idx_node, "MATCHES", pattern_node)
        tx.create(relationship)
        tx.commit()


    def persist_type_and_property(self, _type, _property):
        _property_node = self.persist_property(_property)
        _type_node = self.persist_type(_type)
        tx = self.graph.begin()
        relationship = Relationship(_type_node, "HAS_PROPERTY", _property_node)
        tx.create(relationship)
        tx.commit()


    def persist_pattern_and_property(self, index_pattern, _property):
        """
        :type  index_pattern: IndexPattern
        """
        pattern_node = self.persist_pattern(index_pattern)
        _property_node = self.persist_property(_property)

        tx = self.graph.begin()
        relationship = Relationship(pattern_node, "RELATED_TO_PROPERTY", _property_node)
        tx.create(relationship)
        tx.commit()
