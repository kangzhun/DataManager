# -*- coding: utf-8 -*-
# 将生物三元组写入mongodb
import json
import os

from pymongo import MongoClient
from tqdm import tqdm

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TRIPLE, BIO_TRIPLE_PATH, \
    MONGODB_BIOLOGY_NODE
from logger import BaseLogger
from utils import seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
t_collection = db.get_collection(MONGODB_BIOLOGY_TRIPLE)
n_collection = db.get_collection(MONGODB_BIOLOGY_NODE)

logger = BaseLogger()

node_docs = n_collection.find()  # 读取MONGODB_BIOLOGY_NODE中的数据

logger.debug('start extract triple and write to %s', MONGODB_BIOLOGY_TRIPLE)
for doc in tqdm(node_docs):  # 遍历所有节点，读取节点属性信息，并写入到MONGODB_BIOLOGY_TRIPLE中
    _id = str(doc['_id'])
    for key in doc.keys():
        if key not in ['_id', 'update_time', 'label', 'create_time']:  # 过滤
            if key == 'name':  # 属性为name
                triple = {"node_id": _id,
                          "attribute_name": key,
                          "attribute_date": doc[key]}
            else:  # 属性非name，attribute_date需要进行拼接
                triple = {"node_id": _id,
                          "attribute_name": key,
                          "attribute_date": "\n".join(doc[key])}
            words, tags = seg_doc(triple['attribute_date'])
            triple['attribute_date_index'] = " ".join([w for w in words if w.strip()])
            logger.debug('triple=%s', json.dumps(triple, ensure_ascii=False))
            t_collection.insert(triple)
