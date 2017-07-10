# -*- coding: utf-8 -*-
import json
import os

from pymongo import MongoClient

from config import MONGODB_HOST, MONGODB_PORT, MONGODB_DBNAME, MONGODB_BIOLOGY_PROPERTY, HERE
from logger import BaseLogger
from utils import load_xlsx

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client.get_database(MONGODB_DBNAME)
collection = db.get_collection(MONGODB_BIOLOGY_PROPERTY)

logger = BaseLogger()

SUPERIOR_MAP = {"过程": "实验过程/形成过程/基本过程/发育过程/转化过程/生殖过程/扩增过程",
                "结构": "基本结构/主要结构/细胞结构/内部结构/身体结构",
                "方法": "归类方法/判断方法/选择方法/使用方法/制作方法/解除方法/获取方法/诊断方法/保存方法/表示方法/解析方法/分析方法/止血方法/鉴定方法",
                "特征": "基本特征/喙的特征/足的特征/最突出特征/最显著特征/体质特征",
                "特点": "壁管特点/细胞特点/植被特点/发育特点/生殖特点/化学特点/结构特点/出血特点/遗传特点",
                "关系": "转化关系/有益关系/有害关系",
                "原因": "选材原因/进化原因/形成原因",
                "变化": "胸廓变化/容积变化/能量变化/物质变化",
                "区别": "本质区别",
                "器官": "营养器官/生殖器官/靶器官/分泌器官/产生器官/呼吸器官",
                "观点": "反对观点/赞成观点/主要观点",
                "场所": "产生场所/转化场所/合成场所",
                "方式": "生活方式/散布方式/行为方式/繁殖方式/基本方式/生产方式/变异方式/生殖方式/植物营养方式/捕食方式/营养方式/运动方式/呼吸方式/传递方式/调节方式",
                "条件": "外部条件/自身条件/环境条件/萌发条件/光合条件/分裂条件/产生条件",
                "症状": "缺乏症状",
                "因素": "诱导因素/影响因素/威胁因素",
                "部位": "作用部位/病变部位/合成部位",
                "现象": "休眠现象",
                "方向": "传递方向",
                "层次结构": "上层/中层/下层",
                "基础": "理论基础",
                "标志": "有效标志",
                "举例说明": "否定举例",
                "成分": "组成",
                "去向": "能量去向",
                }


def write2mongodb(path):
    # 从biology_property.xlsx中抽取对象关系和数据文件关系，写入mongodb中
    logger.debug('>>> start write2mongodb')
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    for doc in property_docs:
        name = doc[1].encode('utf-8')
        description = doc[2].encode('utf-8')
        uri = doc[3].encode('utf-8').replace('#-', '_').replace('#', '_').replace('-', '_')
        if "对象属性" in description:
            doc = {'description': description, 'uri': uri,
                   'name': name, 'type': 'object_relationship'}
            logger.debug('got data relationship, doc=%s',
                         json.dumps(doc, ensure_ascii=False))
        else:
            doc = {'description': description, 'uri': uri,
                   'name': name, 'type': 'data_relationship'}
            logger.debug('got object relationship, doc=%s',
                         json.dumps(doc, ensure_ascii=False))
        collection.insert(doc)
    logger.debug('>>> end write2mongodb <<<')


def write2mongodb_v2(path):
    # 从biology_property.xlsx中抽取对象关系和数据文件关系，写入mongodb中
    logger.debug('>>> start write2mongodb')
    property_docs = load_xlsx(path, sheets=[u"property"], start_row=1)
    for doc in property_docs:
        name = doc[1].encode('utf-8').strip()
        description = doc[2].encode('utf-8').strip()
        uri = doc[4].encode('utf-8').upper().strip()
        _type = doc[5].encode('utf-8').strip()

        superior = ""
        for key in SUPERIOR_MAP.keys():
            words = SUPERIOR_MAP[key].split('/')
            if name in words:
                superior = key
        if _type in "对象属性":
            doc = {'description': description, 'uri': uri,
                   'name': name, 'type': 'object', 'superior': superior}
            logger.debug('got data relationship, doc=%s',
                         json.dumps(doc, ensure_ascii=False))
        else:
            doc = {'description': description, 'uri': uri,
                   'name': name, 'type': 'data', 'superior': superior}
            logger.debug('got object relationship, doc=%s',
                         json.dumps(doc, ensure_ascii=False))
        collection.insert(doc)
    logger.debug('>>> end write2mongodb <<<')

if __name__ == '__main__':
    bio_property_path = os.path.join(HERE, 'data/biology_corpus/biology_annotation',
                                     'biology_property.xlsx')
    write2mongodb_v2(bio_property_path)
