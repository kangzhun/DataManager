# -*- coding:utf-8 -*-
# 搜狗细胞词库转txt
import glob
import os

import struct

from config import HERE

SOGOU_LEXICON_PATH = os.path.join(HERE, "data/biology_corpus/sogou_lexicon")
DICTIONARY_PATH = os.path.join(HERE, "data/biology_corpus/dictionary")


def load_sogou_lexicon(path):
    # 加载搜狗细胞词库
    files = glob.glob(r'%s/*.scel' % path)
    for f in files:
        yield f


def read_utf16_str(f, offset=-1, len=2):
    if offset >= 0:
        f.seek(offset)
    return f.read(len).decode('UTF-16LE')


def read_uint16(f):
    return struct.unpack('<H', f.read(2))[0]


def get_word_from_sogou_cell_dict(fname):
    with open(fname, 'rb') as f:
        file_size = os.path.getsize(fname)

        hz_offset = 0
        mask = struct.unpack('B', f.read(128)[4])[0]
        if mask == 0x44:
            hz_offset = 0x2628
        elif mask == 0x45:
            hz_offset = 0x26c4
        else:
            struct.sys.exit(1)

        title = read_utf16_str(f, 0x130, 0x338 - 0x130)
        type = read_utf16_str(f, 0x338, 0x540 - 0x338)
        desc = read_utf16_str(f, 0x540, 0xd40 - 0x540)
        samples = read_utf16_str(f, 0xd40, 0x1540 - 0xd40)

        py_map = {}
        f.seek(0x1540 + 4)

        while 1:
            py_code = read_uint16(f)
            py_len = read_uint16(f)
            py_str = read_utf16_str(f, -1, py_len)

            if py_code not in py_map:
                py_map[py_code] = py_str

            if py_str == 'zuo':
                break

        f.seek(hz_offset)
        while f.tell() != file_size:
            word_count = read_uint16(f)
            pinyin_count = read_uint16(f) / 2

            py_set = []
            for i in range(pinyin_count):
                py_id = read_uint16(f)
                py_set.append(py_map[py_id])
            py_str = "'".join(py_set)

            for i in range(word_count):
                word_len = read_uint16(f)
                word_str = read_utf16_str(f, -1, word_len)
                f.read(12)
                yield py_str, word_str


def show_txt(records):
    for (pystr, utf8str) in records:
        print len(utf8str), utf8str


def store(records, f):
    for (pystr, utf8str) in records:
        f.write("%s\n" % (utf8str.encode("utf8")))


def main(source_path, target_path):
    if os.path.isdir(source_path):
        for root, dirs, files in os.walk(source_path):
            # for fileName in glob.glob(source_path + '*.scel'):
            for file_name in files:
                print 'load', file_name
                generator = get_word_from_sogou_cell_dict(os.path.join(root, file_name))
                with open(target_path, "a") as f:
                    store(generator, f)

    else:
        generator = get_word_from_sogou_cell_dict(source_path)
        with open(target_path, "w") as f:
            store(generator, f)


def scel2txt(path):
    # 将细胞词库转写为txt
    file_name = path.split('/')[-1].replace('.scel', '.txt')
    target_path = os.path.join(DICTIONARY_PATH, file_name)
    with open(target_path, 'w') as fw:
        generator = get_word_from_sogou_cell_dict(path)
        store(generator, fw)


if __name__ == '__main__':
    names = load_sogou_lexicon(SOGOU_LEXICON_PATH)
    for name in names:
        scel2txt(name)
