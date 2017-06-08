# -*- coding: utf-8 -*-
# 对html的生物学科书籍进行清洗，并换为txt
import glob
import os

from bs4 import BeautifulSoup

from config import HERE
from logger import BaseLogger
from utils import seg_doc, _claer

CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/book")
CLEAR_CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/clear_book", "biology_book.txt")
SEG_CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/clear_book", "seg_biology_book.txt")

logger = BaseLogger()


def load_books(path):
    # 加载html格式的生物学科书籍
    logger.debug('>>> start load_books <<<')
    logger.debug('load books from %s', path)
    files = glob.glob('%s/*/Text/*' % path)
    for f in files:
        with open(f, 'r') as fr:
            soup = BeautifulSoup(fr.read(), "lxml")
            yield soup.get_text()
    logger.debug('>>> end load_books <<<')


def save_books_txt(path, doc):
    # 将过滤掉html标签的自然学科文本写入txt文件中
    logger.debug('>>> start save_books_txt <<<')
    logger.debug('write doc to %s', path)
    with open(path, 'a+') as fw:
        fw.write(doc.encode('utf-8'))
    logger.debug('>>> end save_books_txt <<<')


def save_seg_books_txt(corpus_path, target_path):
    with open(corpus_path, 'r') as fr:
        doc = fr.read()
    if doc:
        with open(target_path, 'w') as fw:
            words, flags = seg_doc(doc)
            fw.write(" ".join(words).encode('utf-8'))
    else:
        logger.warn('@@@@@@@@@@@@@@@@@@@@@@@@@@2 read from %s, got None')

if __name__ == '__main__':
    # 将html格式的生物课本，转换为txt格式
    docs = load_books(CORPUS_PATH)
    for doc in docs:
        save_books_txt(CLEAR_CORPUS_PATH, _claer(doc))

    # 得到分词后的生物课本数据
    save_seg_books_txt(CLEAR_CORPUS_PATH, SEG_CORPUS_PATH)

