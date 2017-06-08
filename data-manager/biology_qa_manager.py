# -*- coding: utf-8 -*-
# 生物QA数据清洗
import glob
import json

from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, BIO_QA_FILES_PATH, MONGODB_BIOLOGY_QA
from logger import BaseLogger
from utils import load_xlsx, seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_QA)

logger = BaseLogger()


def write2mongodb(path):
    logger.debug('>>> start write2mongodb <<<')
    triple_docs = load_xlsx(path, start_row=1, start_col=1)
    logger.debug('load from %s, got triple_docs=%s', path, len(triple_docs))
    for doc in triple_docs:
        query = doc[0]
        query_words, query_tags = seg_doc(query)
        answer = doc[1]

        triple_subject = ""
        triple_predicate = ""
        triple_object = ""

        info = {"query": query, "answer": answer,
                "query_index": " ".join(query_words), "triple_subject": triple_subject,
                "triple_predicate": triple_predicate, "triple_object": triple_object}
        logger.debug('info=%s', json.dumps(info))
        collection.insert(info)

if __name__ == "__main__":
    files = glob.glob('%s/*.xlsx' % BIO_QA_FILES_PATH)
    for file_name in files:
        write2mongodb(file_name)
