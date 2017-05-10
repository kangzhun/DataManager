# -*- coding: utf-8 -*-
import json
import os

from pandas import DataFrame
from py2neo import authenticate, Graph, Node, Relationship
from py2neo import PropertyDict
from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TRIPLE, HERE, NEO4J_HOST_PORT, \
    NEO4J_USER, NEO4J_PWD, NEO4J_URL
from const import BIO_CYPER_TEMPLATE
from logger import BaseLogger


class BioKnowledgeDB(BaseLogger):
    def __init__(self, object_relationship, data_relationship, **kwargs):
        super(BioKnowledgeDB, self).__init__(**kwargs)
        self.triple_docs = self._load_triple_docs()
        self.object_relationship = json.load(open(object_relationship, 'r'))
        self.data_relationship = json.load(open(data_relationship, 'r'))
        authenticate(NEO4J_HOST_PORT, NEO4J_USER, NEO4J_PWD)
        self.bio_graph = Graph(NEO4J_URL)

    def _load_triple_docs(self):
        # 从mongodb中加载三元组
        client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client.get_database(MONGODB_DBNAME)
        collection = db.get_collection(MONGODB_BIOLOGY_TRIPLE)
        docs = list(collection.find())
        return docs

    def create_all_nodes(self):
        # 生成node，标签为Biology
        nodes = dict()
        self.debug('[Start create nodes] triple_docs=%s', len(self.triple_docs))
        for doc in self.triple_docs:
            triple_subject = doc.get("triple_subject", "")         # 主语
            triple_predicate = doc.get("triple_predicate", "")     # 谓语
            triple_object = doc.get("triple_object", "")           # 宾语
            predicate_value = self.data_relationship.get(triple_predicate, {}).get("uri", "")
            if triple_subject and triple_subject not in nodes.keys():
                nodes[triple_subject] = PropertyDict({"name": triple_subject})
                if not predicate_value and triple_object:
                    nodes[triple_object] = PropertyDict({"name": triple_subject})
                elif predicate_value and triple_object:
                    nodes[triple_subject][predicate_value] = triple_object
            else:
                if not predicate_value and triple_object:
                    nodes[triple_object] = PropertyDict({"name": triple_subject})
                elif predicate_value and triple_object:
                    nodes[triple_subject][predicate_value] = triple_object
        tx = self.bio_graph.begin()
        for item in nodes.values():
            node = Node('Biology', name=item['name'])
            del item['name']
            if item:
                for key in item.keys():
                    node[key] = item[key]
            tx.create(node)
        tx.commit()

    def create_all_relationships(self):
        tx = self.bio_graph.begin()
        self.debug('[Start create nodes] triple_docs=%s', len(self.triple_docs))
        for doc in self.triple_docs:
            triple_subject = doc.get("triple_object", "")          # 主语
            triple_predicate = doc.get("triple_predicate", "")     # 谓语
            triple_object = doc.get("triple_subject", "")          # 宾语

            node_a = self.bio_graph.find_one(label="Biology", property_key="name",
                                             property_value=triple_subject)
            node_b = self.bio_graph.find_one(label="Biology", property_key="name",
                                             property_value=triple_object)
            if triple_predicate in self.object_relationship.keys() and node_a and node_b:
                predicate_info = self.object_relationship[triple_predicate]
                a_b_relationship = Relationship(node_a, triple_predicate, node_b)
                for key in predicate_info:
                    a_b_relationship[key] = predicate_info[key]
                tx.create(a_b_relationship)
        tx.commit()

    def delete_all(self):
        self.debug("start delete all nodes")
        self.bio_graph.delete_all()

    def return_all_node(self):
        condition = BIO_CYPER_TEMPLATE['all_node']
        data = list(self.bio_graph.run(condition).data())
        self.debug('got %s nodes', len(data))
        return data

    def return_node_property(self, name, node_property):
        data = None
        self.debug('search node name=%s, property=%s', name, node_property)
        if name and node_property:
            condition = BIO_CYPER_TEMPLATE['node_property'] % (name, node_property)
            data = self.bio_graph.run(condition).data()
            self.debug('got property_value=%s', data)
            if not data:
                self.debug('search equal node name=%s, property=%s', name, node_property)
                condition = BIO_CYPER_TEMPLATE['equal_node_property'] % (name, node_property)
                data = self.bio_graph.run(condition).data()
                self.debug('got property_value=%s', data)
        else:
            self.warn('@@@@@@@@@@@@@@ unexpected value!!!!!!')
        return data

    def return_node(self, name):
        data = None
        self.debug('search node name=%s', name)
        if name:
            condition = BIO_CYPER_TEMPLATE['node_data'] % name
            data = self.bio_graph.run(condition).data()
            self.debug('got node_data=%s', data)
        else:
            self.warn('@@@@@@@@@@@@@@ unexpected value!!!!!!')
        return data

    def return_neighbors_property(self, name, relationship, node_property):
        data = None
        self.debug('search node name=%s, relationship=%s, property=%s', name, relationship, node_property)
        if name and node_property:
            condition = BIO_CYPER_TEMPLATE['neighbors_property'] % (name, relationship, node_property)
            data = self.bio_graph.run(condition).data()
            self.debug('got property_value=%s', data)
        else:
            self.warn('@@@@@@@@@@@@@@ unexpected value!!!!!!')
        return data

    def return_neighbors(self, name, relationship):
        data = None
        self.debug('search node name=%s, relationship=%s', name, relationship)
        if name:
            condition = BIO_CYPER_TEMPLATE['neighbors_data'] % (name, relationship)
            data = self.bio_graph.run(condition).data()
            self.debug('got node_data=%s', data)
        else:
            self.warn('@@@@@@@@@@@@@@ unexpected value!!!!!!')
        return data

if __name__ == "__main__":
    bio_object_relation_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                            'object_relation.json')
    bio_data_relation_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                          'data_relation.json')
    db = BioKnowledgeDB(bio_object_relation_path, bio_data_relation_path)
    # db.create_all_nodes()
    # db.create_all_relationships()
    # db.delete_all()

    all_nodes = db.return_all_node()
    for node in all_nodes:
        print node

    node_property = db.return_node_property('哺乳动物', 'biology_character')
    print node_property

    node = db.return_node('生活垃圾')
    print node

    neighbors_property = db.return_neighbors_property('桃花', '组成', 'name')
    print neighbors_property

    neighbors_data = db.return_neighbors('桃花', '组成')
    print neighbors_data
