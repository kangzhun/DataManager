# -*- coding: utf-8 -*-
# 对html的生物学科书籍进行清洗，并换为txt
import glob
import os

from bs4 import BeautifulSoup

from config import HERE
from utils import seg_doc

CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/book")
CLEAR_CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/clear_book", "biology_book.txt")
SEG_CORPUS_PATH = os.path.join(HERE, "data/biology_corpus/clear_book", "seg_biology_book.txt")


def _claer(doc):
    return "\n".join([line for line in doc.replace(u"　", u"").split('\n') if line.strip()])


def load_books(path):
    # 加载html格式的生物学科书籍
    files = glob.glob('%s/*/Text/*' % path)
    for f in files:
        with open(f, 'r') as fr:
            doc = fr.read()
            soup = BeautifulSoup(doc, "lxml")
            yield soup.get_text()


def save_books_txt(path, doc):
    # 将过滤掉html标签的自然学科文本写入txt文件中
    with open(path, 'a+') as fw:
        fw.write(doc.encode('utf-8'))


def save_seg_books_txt(corpus_path, target_path):
    with open(corpus_path, 'r') as fr:
        doc = fr.read()

    if doc:
        with open(target_path, 'w') as fw:
            words, flags = seg_doc(doc)
            fw.write(" ".join(words).encode('utf-8'))

if __name__ == '__main__':
    # docs = load_books(CORPUS_PATH)
    # for doc in docs:
    #     save_books_txt(CLEAR_CORPUS_PATH, _claer(doc))
    save_seg_books_txt(CLEAR_CORPUS_PATH, SEG_CORPUS_PATH)

