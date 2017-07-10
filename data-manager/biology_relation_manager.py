# -*- coding: utf-8 -*-


import datetime
from pymongo import MongoClient
from tqdm import tqdm

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_NODE, BIO_TRIPLE_PATH, \
    MONGODB_BIOLOGY_PROPERTY, MONGODB_BIOLOGY_RELATION
from logger import BaseLogger
from utils import load_xlsx, _claer

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)

node_c = db.get_collection(MONGODB_BIOLOGY_NODE)
property_c = db.get_collection(MONGODB_BIOLOGY_PROPERTY)
relation_c = db.get_collection(MONGODB_BIOLOGY_RELATION)

logger = BaseLogger()

node_property = property_c.find()

object_relation_map = dict()
for p in node_property:
    name = p['name']
    uri = p['uri']
    superior = p['superior']
    _type = p['type']
    if _type == 'object':
        object_relation_map[name] = {'uri': uri, 'superior': superior}

logger.debug('got object_relation=%s', len(object_relation_map))


def write2mongodb(path, sheets=[], start_row=4):
    logger.debug('>>> start write2mongodb <<<')
    logger.debug('load file from %s', path)
    triple_docs = load_xlsx(path, sheets=sheets, start_row=start_row)
    logger.debug('got triple=%s', len(triple_docs))

    now = datetime.datetime.now()
    relations = set()
    logger.debug('start extract info and write into mongodb...')
    for doc in tqdm(triple_docs):  # 遍历三元组，并写入mongodb中
        triple_subject = _claer(doc[0])
        triple_predicate = _claer(doc[1])
        triple_object = _claer(doc[2])

        assert triple_subject
        assert triple_predicate
        assert triple_object

        if triple_predicate in object_relation_map.keys():
            uri = object_relation_map[triple_predicate]['uri']
            source_node = node_c.find({"name": triple_subject}).limit(1)
            target_node = node_c.find({"name": triple_object}).limit(1)
            source_node_id = None
            target_node_id = None
            if source_node:
                source_node_id = list(source_node)[0].get('_id', None)
            if target_node:
                target_node_id = list(target_node)[0].get('_id', None)

            if source_node_id and target_node_id:
                token = str(source_node_id) + str(target_node_id) + uri
                if token not in relations:  # 避免重复关系
                    relations.add(token)
                    doc = {"source_node_name": triple_subject,
                           "target_node_name": triple_object,
                           "source_node_id": source_node_id,
                           "target_node_id": target_node_id,
                           "create_time": now,
                           "update_time": now,
                           "uri": uri}
                    # relation_c.insert(doc)
                else:
                    logger.debug('reduplicate relation, %s--[%s]-->%s',
                                 triple_subject, uri, triple_object)
            else:
                logger.warn('@@@@@@@@@@@@@@@ do not find node=%s or node=%s', triple_subject, triple_object)

if __name__ == "__main__":
    # 将节点信息组写入mongodb
    write2mongodb(BIO_TRIPLE_PATH)
