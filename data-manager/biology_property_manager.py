# -*- coding: utf-8 -*-
import os

from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_PROPERTY, HERE
from utils import load_xlsx

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_PROPERTY)


def write2mongo(path):
    # 从biology_property.xlsx中抽取对象关系和数据文件关系，写入mongodb中
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    for doc in property_docs:
        name = doc[1].encode('utf-8')
        description = doc[2].encode('utf-8')
        uri = doc[3].encode('utf-8').replace("#", "_").replace("-", "_").replace(".", "_")
        if "对象属性" in description:
            doc = {'description': description, 'uri': uri, 'name': name, 'type': 'object_relationship'}
        else:
            doc = {'description': description, 'uri': uri, 'name': name, 'type': 'data_relationship'}
        collection.insert(doc)

if __name__ == '__main__':
    bio_property_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                     'biology_property.xlsx')
    write2mongo(bio_property_path)
