# -*- coding: utf-8 -*-
# 生物问答模板导入
import os
from bs4 import BeautifulSoup

from pymongo import MongoClient

from config import HERE, MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TEMPLATE
from utils import load_xlsx, seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_TEMPLATE)


def load_html(path):
    # 读取模板页面
    with open(path, 'r') as fr:
        soup = BeautifulSoup(fr.read(), "lxml")
    return soup


def generate_relation_attribute_map(path):
    # 从biology_property.xlsx中抽取谓语信息，用于补全模板页面的type
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    relation_attributes = dict()
    for doc in property_docs:
        uri = doc[3].encode('utf-8')
        key = uri.replace("common#", "").replace('biology#', "")
        relation_attributes[key] = {'uri': uri}
    return relation_attributes


def clear_type(content):
    # 清洗谓语（模板页面谓语uri缺损，需要进行补全）
    ret_content = list()
    path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                        'biology_property.xlsx')
    relation_attributes = generate_relation_attribute_map(path)
    for c in content:
        c_text = c.get_text()
        if c_text in relation_attributes.keys():
            c_text = relation_attributes[c_text]['uri'].replace("#", "_").replace("-", "_").replace(".", "_")
            ret_content.append(c_text)
        else:
            ret_content.append(c_text.replace("#", "_").replace("-", "_").replace(".", "_"))
    return ret_content


def clear_p_content(content):
    # 清洗模板页面的正则表达式，并抽取每个正则表达式的关键字
    ret_content = list()
    keywords_list = list()
    for c in content:
        c_text = c.get_text()
        ret_content.append(c_text.replace('(?<', '(?P<'))
        clear_c_text = c_text.replace('(?<title>(.*)?)', '').\
            replace('?<title>', '').\
            replace('.{0,4}', '').\
            replace('.{0,6}', '').\
            replace('(.*)?', '')
        words_str = clear_c_text.replace('(', ' ').replace(')', ' ').replace('?', ' ').replace('|', ' ')
        words, tags = seg_doc(words_str)
        keywords = " ".join(set([w for w in words if w.strip()]))
        keywords_list.append(keywords)
    return ret_content, keywords_list


def write2mongo(pattern_list, key_index_list, predicate_value_list):
    """
    将模板写入到mongodb中
    :param pattern_list: re模板列表
    :param key_index_list: 关键字列表
    :param predicate_value_list: 谓语列表
    :return: 
    """
    for p, k, t in zip(pattern_list, key_index_list, predicate_value_list):
        collection.insert({"pattern": p, "key_index": k, "predicate_value": t})

if __name__ == "__main__":
    html_path = os.path.join(HERE, "data/biology_corpus/template_corpus", "template_file.html")
    doc = load_html(html_path)
    p_content, keywords = clear_p_content(doc.findAll('td', class_='pcontent'))
    p_type = clear_type(doc.findAll('td', class_='type'))

    write2mongo(p_content, keywords, p_type)
