# -*- coding: utf-8 -*-

from datetime import datetime
from py2neo import authenticate, Graph, Node, Relationship
from pymongo import MongoClient
from tqdm import tqdm

from config import NEO4J_HOST_PORT, NEO4J_URL, NEO4J_USER, NEO4J_PWD, MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, \
    MONGODB_BIOLOGY_NODE, MONGODB_BIOLOGY_RELATION
from logger import BaseLogger


class BioKnowledgeDB(BaseLogger):
    def __init__(self):
        super(BioKnowledgeDB, self).__init__()
        authenticate(NEO4J_HOST_PORT, NEO4J_USER, NEO4J_PWD)
        self.bio_graph = Graph(NEO4J_URL)

        client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client.get_database(MONGODB_DBNAME)
        self.node_c = db.get_collection(MONGODB_BIOLOGY_NODE)          # 节点信息库
        self.relation_c = db.get_collection(MONGODB_BIOLOGY_RELATION)  # 节点关系信息库

    def generate_nodes(self):
        """
        生成节点
        :return:
        """
        self.debug('>>> start generate_nodes <<<')
        node_docs = self.node_c.find()
        tx = self.bio_graph.begin()
        for doc in tqdm(node_docs):
            labels = doc['label']
            doc['_id'] = str(doc['_id'])
            doc['create_time'] = str(doc["create_time"])
            doc['update_time'] = str(doc["update_time"])
            del (doc['label'])
            node = Node(*labels, **doc)  # 默认标签标签为“生物概念”
            tx.create(node)
        tx.commit()
        self.debug('>>> end generate_nodes <<<')

    def generate_relations(self):
        """
        生成节点间的关系
        :return:
        """
        self.debug('>>> start generate_relations <<<')
        tx = self.bio_graph.begin()
        relation_docs = self.relation_c.find()
        for doc in tqdm(relation_docs):
            doc['_id'] = str(doc['_id'])
            doc['create_time'] = str(doc["create_time"])
            doc['update_time'] = str(doc["update_time"])
            source_node_id = str(doc['source_node_id'])
            target_node_id = str(doc['target_node_id'])
            uri = doc['uri']

            source_nodes = self.bio_graph.find(label='生物概念', property_key="_id", property_value=source_node_id)
            target_nodes = self.bio_graph.find(label='生物概念', property_key="_id", property_value=target_node_id)
            source_node_list = list(source_nodes)
            target_node_list = list(target_nodes)

            if len(source_node_list) > 1 or len(target_node_list) > 1:  # source_node或target_node为重复节点
                self.error("@@@@@@@@@@@@@@@@@@@ source_node=%s or target_node=%s are duplicate node",
                           source_node_id, target_node_id)
                continue
            if target_node_list and target_node_list:  # source_node和target_node存在
                relation = Relationship(source_node_list[0], uri, target_node_list[0])
                tx.create(relation)
            else:  # source_node和target_node至少有一个不存在
                self.error("@@@@@@@@@@@@@@@@@@@ source_node=%s or target_node=%s don't exits",
                           source_node_id, target_node_id)
        tx.commit()
        self.debug('>>> end generate_relations <<<')

    def clear(self):
        self.debug(">>> start clear <<<")
        self.bio_graph.delete_all()
        self.debug(">>> end clear <<<")


if __name__ == '__main__':
    knowledge_manager = BioKnowledgeDB()
    # knowledge_manager.clear()
    knowledge_manager.generate_nodes()
    knowledge_manager.generate_relations()
