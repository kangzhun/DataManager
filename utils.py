# -*- coding: utf-8 -*-
import jieba
import jieba.posseg as pseg

from config import CUSTOM_DICTIONARY_PATH

jieba.load_userdict(CUSTOM_DICTIONARY_PATH)


def seg_doc(doc):
    # 分词
    words = list()
    tags = list()
    for item in pseg.cut(doc):
        words.append(item.word)
        tags.append(item.flag)
    return words, tags

if __name__ == "__main__":
    print seg_doc("你好啊！我是谁谁谁")
