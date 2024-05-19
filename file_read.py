import os
import re
import sys
import logging
# import tbl_func
from tbl_func import *

from datetime import datetime
# path = "Z:\\총통합\\88.회사\\LDAS 제안서 작업\\분석\\업무분석\\20210531\\20210531"
path = "Z:\\PycharmProjects\\gram_prj\\TEST_FILE\\aaa"
#path = "Z:\\PycharmProjects\\gram_prj\\OASIS\\Oasis\\Oasis-Cloud-Trans\\oasiscloud\\workload"

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
            content = content.split('\n')
            kk  = remove_comments(content)
            # print("@@kk", kk)
            lines = kk.split('\n')  # 파일의 내용을 줄 단위로 분할하여 리스트로 저장

            # 주어진 파일에서 여러 개의 문자열이 포함된 라인을 삭제하고 결과를 반환합니다.
            lines = remove_lines_with_substrings(lines)

            # 주어진 파일에서 테이블과 데이터베이스 이름 매핑을 자동으로 생성합니다.
            var_key_map = generate_table_db_map(lines)
            print("@@JJ2", var_key_map)


            lines = preprocess_contents(var_key_map, lines)
            #print("@@kk2", kk2)
            # 주어진 SQL 문자열에서 placeholders를 실제 값으로 치환합니다.
            # kk2 = replace_placeholders(lines, var_key_map)
            print("#####################")
            #문자열 리스트의 각 항목을 확인하여 행 마지막에 \가 있는 경우 한 줄로 합칩니다.
            combined_lines = combine_lines_with_backslash(lines)
            combined_lines2 = update_save_dir(combined_lines)
            relative_path = os.path.relpath(file_path, path)
            get_df_mapping_rsult = get_df_mapping(relative_path, file_name,combined_lines2)
            #print("combined_lines", combined_lines)
            line_num = 0
            for line in combined_lines2:
                line_num = line_num + 1
                if "'('"   in line:  # 텍스트 확인용
                    pass
                #print("@@combined_lines2", line_num, line)

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
