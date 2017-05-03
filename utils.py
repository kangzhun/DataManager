# -*- coding: utf-8 -*-
import json
import os

import jieba
import jieba.posseg as pseg
import requests
import xlrd
from requests import ConnectionError, HTTPError, Timeout

from config import CUSTOM_DICTIONARY_PATH, HERE

jieba.load_userdict(CUSTOM_DICTIONARY_PATH)


def seg_doc(doc):
    # 分词
    words = list()
    tags = list()
    for item in pseg.cut(doc):
        words.append(item.word)
        tags.append(item.flag)
    return words, tags


def httpagent(**kwargs):
    url = kwargs.get('url', None)
    ret_type = kwargs.get('ct', 'json')
    method = kwargs.get('method', 'GET')
    params = kwargs.get('params', None)
    headers = kwargs.get('headers', None)
    data = kwargs.get('data', None)
    response_on_error = kwargs.get('on_error', None)
    debug = kwargs.get("debug", True)
    enable_cookies = kwargs.get("enable_cookie", False)
    cookie_func = kwargs.get("cookie_func", lambda key: {})
    set_cookie_func = kwargs.get("set_cookie_func", lambda cookie, who: cookie)
    cookie_func_param = kwargs.get("cookie_func_params", None)
    logger = kwargs.get("logger", None)

    try:
        url = url.encode('utf-8')
        req_param = dict()
        req_param["headers"] = headers
        req_param["timeout"] = kwargs.get("timeout", 2)        # default 2s timeout
        req_param["verify"] = False
        if enable_cookies:
            logger.debug("use cookies: True, whose cookie: %s", cookie_func_param)
            req_param["cookies"] = cookie_func(cookie_func_param)
            logger.debug("cookies values: %s", json.dumps(req_param["cookies"], indent=4, sort_keys=True,
                                                          ensure_ascii=False))
        if method == 'GET':
            req_param["params"] = params
            r = requests.get(url, **req_param)
        else:
            req_param["data"] = data
            r = requests.post(url, **req_param)
        if enable_cookies:
            if r.cookies.get_dict():
                logger.debug("set cookie for: %s, cookies: %s", cookie_func_param,
                             json.dumps(r.cookies.get_dict(), indent=4, sort_keys=True, ensure_ascii=False))
                set_cookie_func(r.cookies, cookie_func_param)

        r.raise_for_status()

        logger.debug("request for %s success", url)
        if (params or data) and debug:
            logger.debug("params/data:\n%s", json.dumps(params or data, indent=4, sort_keys=True, ensure_ascii=False))
        if ret_type == 'json':
            try:
                return r.json()
            except Exception, e:
                logger.error("non json response, this is unexpected! ret=%s", r.text[:200])
                return response_on_error
        else:
            return r.text
    except HTTPError, e:
        if response_on_error is not None:
            logger.warn("request for %s got non-2XX result:%s, return pre_set_result:%s", url, e, response_on_error)
            return response_on_error
        else:
            logger.warn("request for %s got non-2XX result:%s", url, e)
            return r.text
    except ConnectionError:
        # TODO connection refuse, timeout, then?
        logger.error("request for %s got connection error", url)
        return response_on_error or {}
    except Timeout:
        logger.error("request for %s timeout!(longer than 2s)", url)
        return response_on_error or {}


def load_xlsx(path, sheets=[], start_row=0, start_col=0):
    work_book = xlrd.open_workbook(path)
    ret = list()
    if sheets:
        for sheet_name in sheets:
            book_sheet = work_book.sheet_by_name(sheet_name)
            for row_index in range(start_row, book_sheet.nrows):
                row_value = book_sheet.row_values(row_index)[start_col:]
                ret.append(row_value)
    else:
        for book_sheet in work_book.sheets():
            for row_index in range(start_row, book_sheet.nrows):
                row_value = book_sheet.row_values(row_index)[start_col:]
                ret.append(row_value)

    return ret

if __name__ == "__main__":
    # 测试jieba分词
    print seg_doc("你好啊！我是谁谁谁")

    # 测试xlsx文件读取
    path = os.path.join(HERE, "data/biology_corpus/biology_annotation", "biologyAnnotationOutput_2017-5-3.xlsx")
    bio_annotation = load_xlsx(path, sheets=[u"标注库三元组"], start_row=4)
    for row in bio_annotation:
        raw_input(" ".join(row))
