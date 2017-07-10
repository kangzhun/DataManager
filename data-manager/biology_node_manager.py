# -*- coding: utf-8 -*-

import datetime
from pymongo import MongoClient
from tqdm import tqdm

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_NODE, BIO_TRIPLE_PATH, \
    MONGODB_BIOLOGY_PROPERTY
from logger import BaseLogger
from utils import load_xlsx, seg_doc, _claer

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
node_c = db.get_collection(MONGODB_BIOLOGY_NODE)

property_c = db.get_collection(MONGODB_BIOLOGY_PROPERTY)

logger = BaseLogger()

node_property = property_c.find()

data_relation_map = dict()
object_relation_map = dict()
for p in node_property:
    name = p['name']
    uri = p['uri']
    superior = p['superior']
    _type = p['type']
    if _type == 'data':
        data_relation_map[name] = {'uri': uri, 'superior': superior}
    else:
        object_relation_map[name] = {'uri': uri, 'superior': superior}

logger.debug('got data_relation=%s, object_relation=%s', len(data_relation_map), len(object_relation_map))


def write2mongodb(path, sheets=[], start_row=4):
    logger.debug('>>> start write2mongodb <<<')
    logger.debug('load file from %s', path)
    triple_docs = load_xlsx(path, sheets=sheets, start_row=start_row)
    logger.debug('got triple=%s', len(triple_docs))

    nodes = dict()  # 使用字典去重节点
    now = datetime.datetime.now()
    logger.debug('start extract info...')
    for doc in tqdm(triple_docs):  # 遍历三元组，并写入mongodb中
        triple_subject = _claer(doc[0])
        triple_predicate = _claer(doc[1])
        triple_object = _claer(doc[2])

        assert triple_subject
        assert triple_predicate
        assert triple_object

        # 生成节点:name字段/label字段：默认为生物概念/create_tie:创建时间/update_time：修改时间
        if triple_predicate in ['label', 'description']:  # 关系属性为label, description, 将triple_subject加入到nodes
            if triple_subject not in nodes.keys():
                nodes[triple_subject] = {"name": triple_subject,
                                         "label": [u"生物概念"],
                                         "create_time": now,
                                         'update_time': now}

        if triple_predicate in data_relation_map.keys():  # 关系为数据属性, 将三元组加入到对应节点中
            uri = data_relation_map[triple_predicate]['uri']
            superior = data_relation_map[triple_predicate]['superior']
            if triple_subject not in nodes.keys():
                nodes[triple_subject] = {"name": triple_subject,
                                         "label": [u"生物概念"],
                                         "create_time": now,
                                         'update_time': now}
            # 将宾语写入到节点中对应属性中
            if uri not in nodes[triple_subject].keys():  # 新属性
                nodes[triple_subject][uri] = [triple_object, ]
            else:  # 已有属性
                if triple_object not in nodes[triple_subject][uri]:  # 去除重复信息
                    nodes[triple_subject][uri].append(triple_object)

            # 将宾语写入节点对应上位属性中
            if superior:
                superior_uri = data_relation_map[superior]['uri']
                if superior_uri not in nodes[triple_subject].keys():  # 新属性
                    nodes[triple_subject][superior_uri] = ["%s---%s" % (triple_predicate, triple_object), ]
                else:  # 已有属性
                    superior_value = "%s---%s" % (triple_predicate, triple_object)
                    if superior_value not in nodes[triple_subject][superior_uri]:
                        nodes[triple_subject][superior_uri].append(superior_value)

        if triple_predicate in object_relation_map.keys():  # 关系为对象属性, 将triple_object加入到nodes
            if triple_subject not in nodes.keys():  # 生成主语节点，若节点存在则不执行
                nodes[triple_subject] = {"name": triple_subject,
                                         "label": [u"生物概念"],
                                         "create_time": now,
                                         'update_time': now}

            # 若关系舒心为[u'分类', u'类型', u'相关分类', u'类别', u'下属于', u'属于']中的，将宾语作为该节点的标签
            if triple_predicate in [u'分类', u'类型', u'相关分类', u'类别', u'下属于', u'属于']:
                if triple_object not in nodes[triple_subject]['label']:
                    nodes[triple_subject]['label'].append(triple_object)

            if triple_predicate in [u'相关人物']:
                if len(nodes[triple_subject]['label']) == 1:
                    nodes[triple_subject]['label'].append(u'人物')

            if triple_object not in nodes.keys():  # 生成宾语节点，若节点存在则不执行
                nodes[triple_object] = {"name": triple_object,
                                        "label": ["生物概念"],
                                        "create_time": now,
                                        'update_time': now}

    logger.debug('got nodes=%s', len(nodes))
    logger.debug('start write into mongodb...')
    for node_key in tqdm(nodes.keys()):
        node_c.insert(nodes[node_key])

if __name__ == "__main__":
    # 将节点信息组写入mongodb
    write2mongodb(BIO_TRIPLE_PATH)
