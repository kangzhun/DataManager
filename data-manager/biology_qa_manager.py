# -*- coding: utf-8 -*-
# 生物QA数据清洗
import glob

from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, BIO_QA_FILES_PATH, MONGODB_BIOLOGY_QA
from utils import load_xlsx, seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_QA)


def write2mongo(path):
    triple_docs = load_xlsx(path, start_row=1, start_col=1)
    for doc in triple_docs:
        query = doc[0]
        query_words, query_tags = seg_doc(query)
        answer = doc[1]

        triple_subject = ""
        triple_predicate = ""
        triple_object = ""

        collection.insert({"query": query,
                           "answer": answer,
                           "query_index": " ".join(query_words),
                           "triple_subject": triple_subject,
                           "triple_predicate": triple_predicate,
                           "triple_object": triple_object})

if __name__ == "__main__":
    files = glob.glob('%s/*.xlsx' % BIO_QA_FILES_PATH)
    for file_name in files:
        write2mongo(file_name)
