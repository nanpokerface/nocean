import os
import re
import sys
import logging
# import tbl_func
from tbl_func import *

from datetime import datetime
# path = "Z:\\총통합\\88.회사\\LDAS 제안서 작업\\분석\\업무분석\\20210531\\20210531"
path = "Z:\\PycharmProjects\\gram_prj\\TEST_FILE\\aaa"


def print_file_contents(file_path, file_name):
    full_file_path = os.path.join(file_path, file_name)
    encoding = 'UTF-8'

    try:
        with open(full_file_path, 'rt', encoding=encoding) as f:
            for line in f:
                pass

    except UnicodeDecodeError:
        encoding = 'latin-1'

    try:
        line_num = 0
        var_key_map = {}
        with open(full_file_path,  'rt', encoding=encoding) as f:
            print(f'File2: {file_name}')
            content = f.read()
            kk  = remove_comments(content)
            # print("@@kk", kk)
            lines = kk.split('\n')  # 파일의 내용을 줄 단위로 분할하여 리스트로 저장
            # 사용 예시
            lines = remove_lines_with_substrings(lines)
            var_key_map = generate_table_db_map(lines)
            print("@@JJ2", var_key_map)

            # for line in f:
            #     line_num += 1
            #     print(f'{line_num}: {line.strip()}')


    except FileNotFoundError:
        print(f'File not found: {full_file_path}')

    except Exception as e:
        print(f'Error occurred: {e}')

for dir_path, dir_names, file_names in os.walk(path):
    # print(f"Directory: {dir_path}")
    for dir_name in sorted(dir_names):
        dir_name_with_path = os.path.join(dir_path, dir_name)
        print(f"Sub-directory: {dir_name_with_path}")
    for file_name in sorted(file_names):
        filename, ext = os.path.splitext(file_name)
        if ext in ['.sql', '.sh', '.py']:
            file_name_with_path = os.path.join(dir_path, file_name)
            # print(f"File: {file_name_with_path}")
            print_file_contents(dir_path, file_name)
