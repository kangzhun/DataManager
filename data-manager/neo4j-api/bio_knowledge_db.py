# -*- coding: utf-8 -*-
import json
import os

import re
from py2neo import authenticate, Graph, Node, Relationship
from py2neo import PropertyDict
from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TRIPLE, HERE, \
    NEO4J_HOST_PORT, NEO4J_USER, NEO4J_PWD, NEO4J_URL
from const import BIO_CYPER_TEMPLATE
from logger import BaseLogger


class BioKnowledgeDB(BaseLogger):
    def __init__(self, object_relationship, data_relationship, default_label, **kwargs):
        """
        初始化
        :param object_relationship: 对象关系json文件路径
        :param data_relationship: 数据关系json文件路径
        :param default_label: 默认标签
        :param kwargs: 
        """
        super(BioKnowledgeDB, self).__init__(**kwargs)
        self.triple_docs = self._load_triple_docs()
        self.object_relationship = json.load(open(object_relationship, 'r'))
        self.data_relationship = json.load(open(data_relationship, 'r'))
        authenticate(NEO4J_HOST_PORT, NEO4J_USER, NEO4J_PWD)
        self.bio_graph = Graph(NEO4J_URL)
        self.default_label = default_label

    def _load_triple_docs(self):
        """
        连接mongodb，并加载三元组
        :return: 
        """
        client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client.get_database(MONGODB_DBNAME)
        collection = db.get_collection(MONGODB_BIOLOGY_TRIPLE)
        docs = list(collection.find())
        return docs

    def create_all_nodes(self):
        """
        从mongodb中抽取三元组，生成node
        :return: 
        """
        nodes = dict()
        self.debug('[Start create nodes] triple_docs=%s', len(self.triple_docs))
        for doc in self.triple_docs:
            triple_subject = doc.get("triple_subject", "")         # 主语
            triple_predicate = doc.get("triple_predicate", "")     # 谓语
            triple_object = doc.get("triple_object", "")           # 宾语

            # triple_predicate是否属于数据关系属性
            property_key = self.data_relationship.get(triple_predicate, {}).get('uri', "")
            if not triple_subject or not triple_object:
                continue
            if triple_subject not in nodes.keys():  # 主语不在nodes中，新建节点
                nodes[triple_subject] = PropertyDict({"name": triple_subject})
                nodes[triple_subject]['label'] = [triple_subject, ]
                if property_key:  # triple_predicate为数据属性，新建节点并添加属性值
                    if triple_object:
                        nodes[triple_subject][property_key] = [triple_object, ]
                    else:
                        self.warn("@@@@@@@@@@@@@@@@@@@@@@@ unexpected triple_object, "
                                  "[subject=%s, predicate=%s, object=%s]",
                                  triple_subject, triple_predicate, triple_object)
                else:  # triple_predicate为关系属性，若triple_object不在nodes中创建新节点
                    if triple_object not in nodes.keys():
                        nodes[triple_object] = PropertyDict({"name": triple_object})
                        nodes[triple_object]['label'] = [triple_object, ]
            else:  # 主语在nodes中，更新节点
                if property_key:  # triple_predicate为数据属性，新建节点并添加属性值
                    if triple_object:
                        is_exist = nodes.get(triple_subject, {}).get(property_key)
                        if is_exist:
                            nodes[triple_subject][property_key].append(triple_object)
                        else:
                            nodes[triple_subject][property_key] = [triple_object, ]
                    else:
                        self.warn("@@@@@@@@@@@@@@@@@@@@@@@ unexpected triple_object, "
                                  "[subject=%s, predicate=%s, object=%s]",
                                  triple_subject, triple_predicate, triple_object)
                else:  # triple_predicate为关系属性，若triple_object不在nodes中创建新节点
                    if triple_object not in nodes.keys():
                        nodes[triple_object] = PropertyDict({"name": triple_object})
        tx = self.bio_graph.begin()
        self.debug("got nodes=%s", len(nodes))
        for item in nodes.values():  # 遍历所有nodes，创建节点
            labels = item.get('label', [])
            node = Node(self.default_label, name=item['name'])  # 默认标签标签为“生物概念”
            if labels:  # 若存在其他label，则添加
                for label in labels:
                    node.add_label(label)
                del (item['label'])
            del(item['name'])
            if item:  # 若节点包含其他属性，则设置属性
                for key in item.keys():
                    node[key] = item[key]
            tx.create(node)
        tx.commit()

    def create_all_relationships(self):
        """
        生成节点之间的关系
        :return: 
        """
        tx = self.bio_graph.begin()
        self.debug('[Start create relationships] triple_docs=%s', len(self.triple_docs))
        for doc in self.triple_docs:  # 遍历所有的三元组
            triple_subject = doc.get("triple_object", "")          # 主语
            triple_predicate = doc.get("triple_predicate", "")     # 谓语
            triple_object = doc.get("triple_subject", "")          # 宾语

            if triple_predicate in self.object_relationship.keys():  # 若关系为对象属性，查询关联节点
                node_a = self.bio_graph.find_one(label=self.default_label, property_key="name",
                                                 property_value=triple_subject)  # 获取节点a
                node_b = self.bio_graph.find_one(label=self.default_label, property_key="name",
                                                 property_value=triple_object)  # 获取节点b
                if node_a and node_b:  # 若节点存在创建联系
                    predicate_info = self.object_relationship[triple_predicate]
                    predicate_info['name'] = triple_predicate
                    a_b_relationship = Relationship(node_a, predicate_info['uri'], node_b)
                    for key in predicate_info:
                        a_b_relationship[key] = predicate_info[key]
                    tx.create(a_b_relationship)
                else:  # 节点不存在
                    if not triple_subject.startswith('http'):
                        self.warn("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ unexpected node_a=%s or node_b=%s not exist!!!!",
                                  triple_subject, triple_object)
        tx.commit()

    def delete_all(self):
        self.debug("start delete all nodes")
        self.bio_graph.delete_all()

    def return_all_node(self):
        condition = BIO_CYPER_TEMPLATE['all_node'] % self.default_label
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
            condition = BIO_CYPER_TEMPLATE['neighbors_property'] % \
                        (self.default_label, name, relationship, node_property)
            data = self.bio_graph.run(condition).data()
            self.debug('got property_value=%s', data)
        else:
            self.warn('@@@@@@@@@@@@@@ unexpected value!!!!!!')
        return data

    def return_neighbors(self, name, relationship):
        data = None
        self.debug('search node name=%s, relationship=%s', name, relationship)
        if name:
            condition = BIO_CYPER_TEMPLATE['neighbors_data'] % (self.default_label, name, relationship)
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
    db = BioKnowledgeDB(bio_object_relation_path, bio_data_relation_path, 'biology')
    db.create_all_nodes()
    db.create_all_relationships()
    # db.delete_all()

    # all_nodes = db.return_all_node()
    # for node in all_nodes:
    #     print node
    # print len(all_nodes)
    #
    # node_property = db.return_node_property('大脑皮层', 'biology_function')
    # print node_property
    #
    # node = db.return_node('生活垃圾')
    # print node
    #
    # neighbors_property = db.return_neighbors_property('桃花', 'common_consistedOf', 'name')
    # print neighbors_property
    #
    # neighbors_data = db.return_neighbors('桃花', 'common_consistedOf')
    # print neighbors_data
