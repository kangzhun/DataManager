# -*- coding: utf-8 -*-
import os

HERE = os.path.abspath(os.path.dirname(__file__))

# 语料资源路径
SOGOU_LEXICON_PATH = os.path.join(HERE, "data/biology_corpus/sogou_lexicon")
DICTIONARY_PATH = os.path.join(HERE, "data/biology_corpus/dictionary")
CUSTOM_DICTIONARY_PATH = os.path.join(HERE, "data/biology_corpus/dictionary/custom_dictionary.txt")
BIO_TRIPLE_PATH = os.path.join(HERE, "data/biology_corpus/biology_annotation",
                               "biologyKnowledgebaseOutput.xlsx")
BIO_QA_FILES_PATH = os.path.join(HERE, "data/biology_corpus/biology_questions")

# logger config
LOGGER_PATH = HERE
LOGGER_NAME = "data_manager.log"

# mongodb config
MONGODB_HOST = "127.0.0.1"
MONGODB_PORT = 27017
MONGODB_DBNAME = "biology-db"
MONGODB_BIOLOGY_TRIPLE = "biology-triple"
MONGODB_BIOLOGY_QA = "biology-qa"
MONGODB_BIOLOGY_TEMPLATE = "biology-template"
MONGODB_BIOLOGY_PROPERTY = 'biology-property'
MONGODB_BIOLOGY_NODE = 'biology-node'
MONGODB_BIOLOGY_RELATION = 'biology-relation'
MONGODB_TEST_CORPUS = 'biology-test_corpus'

# solr config
SOLR_HOST = "127.0.0.1"
SOLR_PORT = 8983
SOLR_SERVER = "http://%s:%s/solr" % (SOLR_HOST, SOLR_PORT)

BIOLOGY_TRIPLE_CORE_NAME = "biology-triple"
BIOLOGY_TRIPLE_CORE = "/".join([SOLR_SERVER, BIOLOGY_TRIPLE_CORE_NAME])

BIOLOGY_QA_CORE_NAME = "biology-qa"
BIOLOGY_QA_CORE = "/".join([SOLR_SERVER, BIOLOGY_QA_CORE_NAME])

SOLR_CORE_MAP = {
    BIOLOGY_TRIPLE_CORE_NAME: BIOLOGY_TRIPLE_CORE,
    BIOLOGY_QA_CORE_NAME: BIOLOGY_QA_CORE,
}

SOLR_DEFAULT_ROWS = 50

# neo4j config
NEO4J_HOST_PORT = "localhost:7475"
NEO4J_USER = "neo4j"
NEO4J_PWD = "741953"
NEO4J_URL = "http://localhost:7475/db/data/"
