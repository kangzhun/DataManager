# -*- coding: utf-8 -*-
# 生物问答模板导入
import json
import os
from bs4 import BeautifulSoup

import re
from pymongo import MongoClient

from config import HERE, MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_TEMPLATE, MONGODB_BIOLOGY_PROPERTY
from logger import BaseLogger
from utils import load_xlsx, seg_doc

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
template_collection = db.get_collection(MONGODB_BIOLOGY_TEMPLATE)
relation_collection = db.get_collection(MONGODB_BIOLOGY_PROPERTY)
logger = BaseLogger()


def load_html(path):
    # 读取模板页面
    logger.debug('>>> start load_html <<<')
    logger.debug('load template_html from path=%s', path)
    with open(path, 'r') as fr:
        soup = BeautifulSoup(fr.read(), "lxml")
    logger.debug('>>> end load_html <<<')
    return soup


def generate_relation_attribute_map(path):
    # 从biology_property.xlsx中抽取谓语信息，用于补全模板页面的type
    logger.debug('>>> start generate_relation_attribute_map <<<')
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    relation_attributes = dict()
    logger.debug('load biology_property from path=%s, got property_docs=%s', path, len(property_docs))
    for doc in property_docs:
        uri = doc[3].encode('utf-8').replace('#-', '_').replace('#', '_').replace('-', '_')
        key = uri.replace("common_", "").replace('biology_', "")
        print key, uri
        if key in relation_attributes.keys():
            relation_attributes[key]['uri'].append(uri)
        else:
            relation_attributes[key] = {'uri': [uri, ]}
        relation_attributes[uri] = {'uri': [uri, ]}
    logger.debug('>>> end generate_relation_attribute_map <<<')
    return relation_attributes


def clear_type(content, priority):
    # 清洗谓语（模板页面谓语uri缺损，需要进行补全）
    logger.debug('>>> start clear_type <<<')
    clear_content = list()
    path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                        'biology_property.xlsx')
    relation_attributes = generate_relation_attribute_map(path)
    for c, p in zip(content, priority):
        c_text = c.get_text().replace('#-', '_').replace('#', '_').replace('-', '_')
        if c_text.startswith('_'):
            c_text = re.sub('^_', '', c_text)
        if p == 1:  # 模板优先级为1， 高优先级模板
            if c_text in relation_attributes.keys():
                uri = relation_attributes[c_text]['uri']
                clear_content.append(uri)
            else:
                logger.warn('@@@@@@@@@@@@@@@@@@@@@@@@@ %s not in relation_attributes', c_text)
                clear_content.append([])
        else:  # 模板优先级为2， 通常为多个predicate的公用模板
            logger.debug('got template %s', c_text)
            clear_content.append([c_text])
    logger.debug('>>> end clear_type <<<')
    return clear_content


def clear_p_content(content):
    # 清洗模板页面的正则表达式，并抽取每个正则表达式的关键字
    logger.debug('>>> start clear_p_content <<<')
    ret_content = list()
    keywords_list = list()
    for c in content:  # 遍历模板将java格式的正则表达式改写为python格式
        c_text = c.get_text()
        ret_content.append(c_text.replace('(?<', '(?P<'))
        clear_c_text = c_text.replace('(?<title>(.*)?)', '').\
            replace('?<title>', '').\
            replace('.{0,4}', '').\
            replace('.{0,6}', '').\
            replace('(.*)?', '')
        # 抽取正则表达式的中文字符串，用于过滤无关模板
        words_str = clear_c_text.replace('(', ' ').replace(')', ' ').replace('?', ' ').replace('|', ' ')
        words, tags = seg_doc(words_str)
        keywords = " ".join(set([w for w in words if w.strip()]))
        keywords_list.append(keywords)
    logger.debug('>>> end clear_p_content <<<')
    return ret_content, keywords_list


def extract_info(docs):
    """
    抽取模板页面信息（包括：content，is_subject,priority,predicate_value,priority）
    :param docs:
    :return:
    """
    logger.debug('>>> start extract_info <<<')
    patterns, keywords = clear_p_content(docs.findAll('td', class_='pcontent'))
    missing_tuple = []
    for doc in docs.findAll('td', class_='subject'):
        if doc.get_text == u'TRUE':
            missing_tuple.append(u'subject')
        else:
            missing_tuple.append(u'object')
    priority = [int(doc.get_text()) for doc in docs.findAll('td', class_='priority')]
    predicates = clear_type(docs.findAll('td', class_='type'), priority)
    info_list = []
    for i in range(len(patterns)):
        info = {'pattern': patterns[i], 'key_index': keywords[i], 'predicates': predicates[i],
                'priority': priority[i] + 1, 'missing_tuple': missing_tuple[i]}
        info_list.append(info)
        logger.debug('logger=%s', json.dumps(info, ensure_ascii=False))
    logger.debug('>>> end extract_info <<<')
    return info_list


def write2mongo(info_list):
    """
    将模板数据写入到mongodb中
    :param info_list:
    :return: 
    """
    for info in info_list:
        template_collection.insert(info)


def save2csv():
    templats_docs = template_collection.find()
    relation_docs = relation_collection.find()
    relations = [doc.get('uri', "") for doc in relation_docs if doc.get('uri', "")]
    for doc in templats_docs:
        doc_list = []
        _id = str(doc['_id'])
        predicate = doc['predicates']
        key_index = doc['key_index']
        pattern = doc['pattern']
        if predicate:
            if predicate[0] not in relations:
                print _id, predicate



if __name__ == "__main__":
    # # 写入mongodb
    # html_path = os.path.join(HERE, "data/biology_corpus/template_corpus", "template_file.html")
    # _docs = load_html(html_path)
    # _info_list = extract_info(_docs)
    # write2mongo(_info_list)

    # mongodb导出
    save2csv()
