import os
import re
import sys
import logging
from datetime import datetime
# path = "Z:\\총통합\\88.회사\\LDAS 제안서 작업\\분석\\업무분석\\20210531\\20210531"
path = "Z:\\PycharmProjects\\gram_prj\\TEST_FILE\\aaa"
w_file_nm = "test_anlyze_kkk_10.dat"
if os.path.exists(w_file_nm):
  os.remove(w_file_nm)

import sqlite3

# 데이터베이스 파일 경로
db_file = 'C:\\Users\\nanpo\\AppData\\Roaming\\DBeaverData\\workspace6\\.metadata\\sample-database-sqlite-1\\Chinook.db'

# sqlite connection
conn = sqlite3.connect(db_file)
c = conn.cursor()

delete_table_query = ''' delete from pgm_tbl_map '''

# c.execute(delete_table_query)
conn.commit()

log_file = "test_anlyze_kkk_5.log"
schema_list = ["BMT","SWG","TMP","SMS","BCR","CDR","ACT","BCM","ACC","ACO","BDWSOR","RSLT","ACP","ADH","ABL","TAS","TBMT","RTN",]

# 로거와 파일핸들러 정의
logger = logging.getLogger(__name__)
filhandler = logging.FileHandler(log_file)

# 로거 실행 설정
logger.setLevel(logging.INFO)
# file_handler.setLevel(logging.INFO)


# 로거에 핸들러 추가
# logger.addHandler(file_handler)

# 로깅 실행
logger.info('Script started')


local_vars = {}
STRD_DT = datetime.now().strftime('%Y%m%d')
# print(STRD_DT)


def remove_comments(sql):
    # /* */ 주석 처리
    # sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    sql = re.sub(r'/\*(.*?)\*/', ' ', sql, flags=re.DOTALL)
    # -- 주석 처리
    sql = re.sub(r'--.*', ' ', sql)
    # # 주석 처리
    sql = re.sub(r'#.*', ' ', sql)
    return sql.strip()

# 괄호 묶기를 위한 함수 정의
def enclose_parentheses(sql):
    # ( 시작과 끝을 묶어주기
    sql = re.sub(r'\(', r' ( ', sql)
    sql = re.sub(r'\)', r' ) ', sql)
    return sql.strip()

# SQL 문장 묶기를 위한 함수 정의
def enclose_sql(sql):
    # spark.sql( 에 매칭되는 ) 찾기
    match = re.search(r'spark\.sql\((.*?)\)', sql)
    if match:
        start_idx = match.start()
        end_idx = match.end()
        sql = sql[:start_idx] + ' ' + sql[start_idx:end_idx] + ' ' + sql[end_idx:]
    return sql.strip()

# ( )를 묶어서 리스트로 만드는 함수 정의
def enclose_parentheses_to_list(content):
    result = []
    stack = []
    for char in content:
        if char == '(':
            stack.append(char)
        elif char == ')':
            if stack:
                stack.pop()
                if not stack:
                    result.append(content[content.find('('):content.find(')')+1])
        else:
            continue
    return result


def map_sql_queries_to_dataframes(sentence):
    # 시작 괄호와 끝 괄호 사이의 SQL 쿼리 추출
    start_index = sentence.find("(")
    end_index = sentence.rfind(")")
    if start_index != -1 and end_index != -1 and start_index < end_index:
        sql_query = sentence[start_index + 1: end_index].strip()
    else:
        return None

    # 데이터프레임 이름 추출
    dataframe_name = re.findall(r'\b\w+\b\s*=', sentence)[0].strip()

    return {dataframe_name: sql_query}


def find_matching_brackets(sentence):
    start_index = -1
    end_index = -1
    open_brackets = 0
    for i, char in enumerate(sentence):
        if char == '(':
            if open_brackets == 0:
                start_index = i
            open_brackets += 1
        elif char == ')':
            open_brackets -= 1
            if open_brackets == 0:
                end_index = i
                break
    return start_index, end_index


def map_sql_queries_to_dataframes(sentence):
    # 괄호 쌍 찾기
    start_index, end_index = find_matching_brackets(sentence)
    if start_index == -1 or end_index == -1:
        return "ERROR: 시작 괄호 또는 끝 괄호를 찾을 수 없습니다."

    # 시작 괄호와 끝 괄호 사이의 SQL 쿼리 추출
    sql_query = sentence[start_index + 1: end_index].strip()

    # 데이터프레임 이름 추출
    dataframe_name_match = re.search(r'\b(\w+)\s*=', sentence[:start_index])
    if dataframe_name_match:
        dataframe_name = dataframe_name_match.group(1)
    else:
        return "ERROR: 데이터프레임 이름을 찾을 수 없습니다."

    return {dataframe_name: sql_query}

def find_dataframe_sql_pairs(filename):
    dataframe_mapping = {}
    with open(filename, 'r') as file:
        lines = file.readlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#"):
                # 주석 라인은 무시
                i += 1
                continue
            elif line.startswith("df_"):
                # 데이터프레임 발견
                dataframe_name = line.split("=")[0].strip()
                sql_query = ""
                # 다음 줄부터 SQL 쿼리 찾기
                i += 1
                while i < len(lines):
                    if "spark.sql(" in lines[i]:
                        # SQL 쿼리 시작점 찾기
                        start_index = lines[i].find("spark.sql(")
                        sql_query += lines[i][start_index:]
                        # SQL 쿼리 종료점 찾기
                        while ")" not in lines[i]:
                            i += 1
                            sql_query += lines[i]
                        dataframe_mapping[dataframe_name] = sql_query.strip()
                        break
                    else:
                        sql_query += lines[i]
                    i += 1
            i += 1
    return dataframe_mapping


def write_to_file(file_path, file_name, line_num, TB_TYPE_CD, tuple_str, line_ori ):
  global w_file_nm
  global path
  # print("pathpath",path)
  # print("file_path",file_path )
  relative_path = os.path.relpath(file_path, path )
  with open(w_file_nm, mode = 'a') as f:
    f.write(f"{STRD_DT}|{relative_path}|{file_name}|{line_num}|{TB_TYPE_CD}|{tuple_str}|{line_ori}\n")




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
        with open(full_file_path,  'rt', encoding=encoding) as f:
            # print(f'File2: {file_name}')
            # print(f.read())
            content = f.read()

            # SQL 쿼리와 데이터프레임 매핑
            dataframe_mapping = map_sql_queries_to_dataframes(content)

            # 결과 출력
            if isinstance(dataframe_mapping, dict):
                for dataframe_name, sql_query in dataframe_mapping.items():
                    print(f"{dataframe_name}:")
                    print(sql_query)
            else:
                print(dataframe_mapping)




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
