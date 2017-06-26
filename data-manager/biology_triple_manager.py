# -*- coding: utf-8 -*-
# 将生物三元组写入mongodb
import json
import os

from pymongo import MongoClient
from tqdm import tqdm

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TRIPLE, BIO_TRIPLE_PATH
from logger import BaseLogger
from utils import load_xlsx, seg_doc, _claer

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_TRIPLE)

logger = BaseLogger()


def write2mongodb(path, sheets=[], start_row=4):
    logger.debug('>>> start write2mongodb <<<')
    logger.debug('load from %s', path)
    triple_docs = load_xlsx(path, sheets=sheets, start_row=start_row)
    for doc in triple_docs:
        if doc:
            triple_subject = _claer(doc[0])
            triple_subject_words, triple_subject_tags = seg_doc(triple_subject)
            triple_predicate = _claer(doc[1])
            triple_predicate_words, triple_predicate_tags = seg_doc(triple_predicate)
            triple_object = _claer(doc[2])
            triple_object_words, triple_object_tags = seg_doc(triple_object)
            if triple_predicate not in [u'出处']:
                info = {"triple_subject": triple_subject,
                        "triple_predicate": triple_predicate,
                        "triple_object": triple_object,
                        "triple_subject_index": " ".join(triple_subject_words),
                        "triple_predicate_index": " ".join(triple_predicate_words),
                        "triple_object_index": " ".join(triple_object_words)}
                collection.insert(info)
            else:
                logger.debug('@@@@@@@@@@@@@@@@@@@@@@@ ignore triple_doc')
        else:
            logger.warn('@@@@@@@@@@@@@@@@@@@@@@@ unexpected values doc is None')
    logger.debug('>>> end write2mongodb <<<')


def triple_count(target_path):
    logger.debug('>>> start triple_count <<<')
    triple_docs = collection.find()
    subjects = set()
    predicates = set()
    logger.debug('got triple_docs=%s', len(triple_docs))
    for doc in triple_docs:
        subject = doc.get('triple_subject', '')
        predicate = doc.get('triple_predicate', '')
        try:
            assert subject
            assert predicate
            subjects.add(subject)
            predicates.add(predicate)
        except Exception, e:
            logger.error(e)
            logger.error('@@@@@@@@@@@@@@@@@ unexpected values doc=%s',
                         json.dumps(doc, ensure_ascii=False))

    logger.debug('got subjects=%s, predicates=%s', len(subjects), len(predicates))

    with open(os.path.join(target_path, 'subjects.txt'), 'w') as fr:
        for item in subjects:
            fr.write(item.encode('utf-8'))
            fr.write('\n')

    with open(os.path.join(target_path, 'subjects.txt'), 'w') as fr:
        for item in predicates:
            fr.write(item.encode('utf-8'))
            fr.write('\n')

if __name__ == "__main__":
    # 将三元组写入mongodb
    write2mongodb(BIO_TRIPLE_PATH)

    # # 从mongodb导出三元组并进行简单统计
    # target_path = os.path.join()
    # triple_count()
