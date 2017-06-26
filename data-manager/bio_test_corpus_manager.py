# -*- coding: utf-8 -*-
# 生物问答模板导入
import glob
import json

from pymongo import MongoClient

from config import  MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_TEST_CORPUS, BIO_QA_FILES_PATH
from logger import BaseLogger
from utils import load_xlsx

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
test_corpus_collection = db.get_collection(MONGODB_TEST_CORPUS)
logger = BaseLogger()


def write2mongodb(path):
    logger.debug('>>> start write2mongodb <<<')
    qa_docs = load_xlsx(path, start_row=1, start_col=1)
    logger.debug('load from %s, got qa_docs=%s', path, len(qa_docs))
    for doc in qa_docs:
        query = doc[0]
        answer = doc[1]
        if not isinstance(answer, unicode):
            answer = unicode(answer)
        info = {"query": query, "answer": answer}
        logger.debug('info=%s', json.dumps(info))
        test_corpus_collection.insert(info)

if __name__ == "__main__":
    files = glob.glob('%s/*.xlsx' % BIO_QA_FILES_PATH)
    for file_name in files:
        write2mongodb(file_name)
