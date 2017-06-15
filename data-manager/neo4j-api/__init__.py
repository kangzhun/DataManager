# -*- coding: utf-8 -*-
import json
import os
from pprint import pprint

from config import HERE
from utils import load_xlsx


def statistics_triple_info(path, subject_set_path, predicate_set_path):
    # 统计三元组信息，包括三元组数量，主语个数，关系属性个数
    triple_docs = load_xlsx(path, sheets=[u"标注库三元组"], start_row=4)
    len_docs = len(triple_docs)
    subject_set = set()
    predicate_set = set()
    for doc in triple_docs:
        triple_subject = doc[0]
        triple_predicate = doc[1]
        subject_set.add(triple_subject)
        predicate_set.add(triple_predicate)
    print "got triple_docs=%i, triple_subjects=%i, triple_predicate=%i" % (len_docs, len(subject_set), len(predicate_set))
    with open(subject_set_path, 'w') as fw:
        for item in subject_set:
            fw.write(item.encode('utf-8')+'\n')

    with open(predicate_set_path, 'w') as fw:
        for item in predicate_set:
            fw.write(item.encode('utf-8')+'\n')


def generate_relation_attribute_map(path, object_path, data_path):
    # 从biology_property.xlsx中抽取对象关系和数据文件关系，写入json文件中
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    object_relation_attribute = dict()
    data_relation_attribute = dict()
    for doc in property_docs:
        _id = doc[0].encode('utf-8')
        name = doc[1].encode('utf-8')
        description = doc[2].encode('utf-8')
        uri = doc[3].encode('utf-8').replace('#-', '_').replace('#', '_').replace('-', '_')
        if "对象属性" in description:
            object_relation_attribute[name] = {'_id': _id, 'description': description, 'uri': uri}
        else:
            data_relation_attribute[name] = {'_id': _id, 'description': description, 'uri': uri}

    json.dump(object_relation_attribute, open(object_path, 'w'))
    json.dump(data_relation_attribute, open(data_path, 'w'))


if __name__ == '__main__':
    # 统计三元组信息
    # bio_annotation_triple_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation', 'biologyAnnotationOutput_2017-5-3.xlsx')
    # subject_save_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation', 'bio_triple_subject.txt')
    # predicate_save_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation', 'bio_triple_predicate.txt')
    # statistics_triple_info(bio_annotation_triple_path, subject_save_path, predicate_save_path)

    # 生成关系属性字典文件
    bio_property_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                     'biology_property.xlsx')
    bio_object_relation_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                            'object_relation.json')
    bio_data_relation_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                          'data_relation.json')
    generate_relation_attribute_map(bio_property_path,
                                    bio_object_relation_path,
                                    bio_data_relation_path)

    # 加载relation_attribute_map
    object_relation = json.load(open(bio_object_relation_path, 'r'))
    pprint(object_relation)
    print '#' * 100
    data_relation = json.load(open(bio_data_relation_path, 'r'))
    pprint(data_relation)
