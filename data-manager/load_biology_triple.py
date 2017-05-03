# -*- coding: utf-8 -*-
# 将生物三元组写入mongodb
from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TRIPLE, BIO_TRIPLE_PATH
from utils import load_xlsx, seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_TRIPLE)


def write2mongo(path):
    triple_docs = load_xlsx(path, sheets=[u"标注库三元组"], start_row=4)
    for doc in triple_docs:
        triple_subject = doc[0]
        triple_subject_words, triple_subject_targs = seg_doc(triple_subject)
        triple_predicate = doc[1]
        triple_predicate_words, triple_predicate_targs = seg_doc(triple_predicate)
        triple_object = doc[2]
        triple_object_words, triple_object_targs = seg_doc(triple_object)
        collection.insert({"triple_subject": triple_subject,
                           "triple_predicate": triple_predicate,
                           "triple_object": triple_object,
                           "triple_subject_index": " ".join(triple_subject_words),
                           "triple_predicate_index": " ".join(triple_predicate_words),
                           "triple_object_index": " ".join(triple_object_words)})

if __name__ == "__main__":
    write2mongo(BIO_TRIPLE_PATH)
