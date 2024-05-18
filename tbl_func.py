from __future__ import print_function
import sqlparse
import re
from collections import namedtuple
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, Punctuation

TableReference = namedtuple(
    "TableReference", ["schema", "name", "alias", "is_function"]
)
TableReference.ref = property(
    lambda self: self.alias
    or (
        self.name
        if self.name.islower() or self.name[0] == '"'
        else '"' + self.name + '"'
    )
)


def remove_comments_xx(content):
    """
    주어진 파일 내용에서 주석을 제거하고 반환합니다.

    Args:
        content (str): 파일 내용

    Returns:
        str: 주석이 제거된 파일 내용
    """
    lines = content.splitlines()
    cleaned_lines = []

    for line in lines:
        # 한 줄 주석 제거
        if line.strip().startswith('#') or line.strip().startswith('//'):
            continue

        # 블록 주석 제거
        if '/*' in line and '*/' in line:
            start = line.index('/*')
            end = line.index('*/') + 2
            line = line[:start] + line[end:]
        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


def remove_lines_with_substrings(file):
    """
    주어진 파일에서 여러 개의 문자열이 포함된 라인을 삭제하고 결과를 반환합니다.

    Args:
        full_file_path (str): 파일의 전체 경로
        encoding (str): 파일의 인코딩 방식
        substrings (list): 삭제할 라인에 포함된 문자열들의 리스트

    Returns:
        str: 수정된 파일 내용
    """

    substrings = ['import ', 'appName(', '.config(', '.getArgs', 'ArgumentParser',  'SparkSession', 'os.path.basename',  'result_data', 'set hive.', 'job_rslt_cd', 'job_st_dtm'
        , 'fail_message', '.setDevLog', 'writeStartExecLog', '.builder', 'enableHiveSupport','getOrCreate','print(', 'except Exception', 'finally:', 'exit(0)' , 'else:', 'try:', 'add jar', 'create temporary function']
    filtered_lines = []

    for line in file:
        include_line = True
        for substring in substrings:
            if substring in line:
                include_line = False
                break
        if include_line:
            filtered_lines.append(line)
    # print("remove_lines_with_substrings1", filtered_lines)
    return filtered_lines

def get_name(var_name, value):
    """
    주어진 문자열에서 스키마 이름을 추출합니다.

    Args:
        str (str): 스키마 이름이 포함된 문자열

    Returns:
        str: 추출된 스키마 이름
    """
    schema_name = value
    if '.' in value:
        schema_name = value.split('.')[1]
    schema_name = schema_name.replace('}', '')
    if "DB" in var_name:
        if "_" in schema_name:
            if "IP_" in schema_name:
                schema_name = "IP_PLF"
            else:
                schema_name = schema_name.split('_')[0]
    print("@@get_name", schema_name)
    return schema_name

def generate_table_db_map(file):
    """
    주어진 파일에서 테이블과 데이터베이스 이름 매핑을 자동으로 생성합니다.

    Args:
        file_path (str): 매핑 정보가 포함된 파일 경로

    Returns:
        dict: 테이블과 데이터베이스 이름의 매핑
    """
    var_key_map = {}
    # print("$$file", type(file),file)  # list 타입으로 전달됨

    for line in file:
        # print("tbl_func_line223", line)
        if ('=' in line and line.count('=') == 1 and '(' not in line and '\\' not in line and 'WHERE ' not in line.upper() and 'ON ' not in line.upper() and 'AND' not in line.upper()
                and 'CASE ' not in line.upper()  and 'WHEN ' not in line.upper() and 'END' not in line.upper() and 'SAVE_DIR' not in line.upper()  )    :  #
            # print("tbl_func_line11", line)
            # print("###generate_table_db_map1", line)
            var_name, value = line.split('=')
            var_name = var_name.strip()

            value = value.replace('"', '').replace("'", '')
            value = value.strip()
            value_ori = value
            print("value", value)
            value = get_name(var_name, value)
            # print("###generate_table_db_map2", line,  var_name, value, value_ori)

            var_key_map[var_name] = value


            # if var_name.startswith('TABLE'):
            #     table_name = value
                # if 'DB' in line:
                #     db_name = table_db_map.get('DB', None)
                # else:
                #     db_name = None
                # table_db_map[table_name] = db_name
            # elif var_name.startswith('DB'):
            #     db_name = value
            #     table_db_map['DB'] = db_name

            # print("&&value", "var_name-", var_name, "value-", value)
    return var_key_map


def preprocess_contents(key_map, file):
    def replace_placeholder1(match):
        placeholder = match.group()
        parts = placeholder.split('.')
        # print("preprocess_placeholder1", parts)
        if len(parts) > 1:
            #print("preprocess_placeholder_여긴나오나", parts[1])
            parts[1] = parts[1].replace("}", "")  # 'list' object has no attribute 'replace'
            return f"{{{parts[1]}}}"
        else:
            # print("preprocess_placeholder2", placeholder)
            return placeholder

    def replace_placeholder2(match):
        placeholder = match.group()
        for key, value in key_map.items():
            if key in placeholder:
                if '.' in placeholder:
                    return f"{{{value}_{placeholder.split('.')[-1]}}}"
                else:
                    return f"{{{value}}}"
        return placeholder

    # 패턴 찾기 - {MartCfg.ACT_T}.CT_KIDS_HOUS_SEG_MONTH  -> {ACT_T}.CT_KIDS_HOUS_SEG_MONTH
    pattern = re.compile(r'\{[\w.]+\}\.\{?\w+\}?')
    filtered_lines = []
    line_num = 0
    for line in file:
        line_num  = line_num + 1
        if re.search(pattern, line):
            match = pattern.findall(line)
            if match:
                line = re.sub(r'\{[^}]+\.\w+\}', replace_placeholder1, line)

        line = re.sub(r'\{[^}]+\}', replace_placeholder2, line)
        if "_lower()"  in line:
            pass
            # print("@@_lower()", line_num, line)

        line = line.replace("_lower()}","")
        line = line.replace("CommonCfg.","")
        line = line.replace("MartCfg.","")
        line = line.replace("DwCfg.","")
        line = re.sub(r"\t", "    ", line)  # TAB 값을 공백으로
        line = line.rstrip()  # 오른쪽 공백 제거
        if 'SPARK.STOP()' not in line.upper()  and '.WRITEDEVLOG' not in line.upper() and 'JOB_END_DTM' not in line.upper() and 'SPARK.UDF.' not in line.upper()  :
            if line.strip():
                filtered_lines.append(line)
                #print("preprocess_contents", line_num, line)

    return filtered_lines


#문자열 리스트의 각 항목을 확인하여 행 마지막에 \가 있는 경우 한 줄로 합칩니다.
def combine_lines_with_backslash(lines):

    result = []
    current_line = ""
    for line in lines:
        if line.endswith("\\"):
            if "=" in line:
                current_line += line[:-1].strip() + " "
            else:
                current_line += line[:-1].strip()
        else:
            if current_line:
                current_line += line.strip()
                result.append(current_line)
                current_line = ""
            else:
                result.append(line)
    return result


#save_dir을 정의하고, 후행 요소에서 해당 문자열을 사용하도록
def update_save_dir(lines):

    result = []
    save_dir_var = ""
    for line in lines:
        if "save_dir" in line and "=" in line:
            save_dir_var = "save_dir-" + line.split("=")[1].strip()
        #print("####save_dir_var", save_dir_var)

        if  line.startswith("df_") and ".write" in line:
            #print("@@@save_dir_var", save_dir_var, line)
            line = line.replace("save_dir", save_dir_var)
        if not ("save_dir" in line and "=" in line):
            result.append(line)
    return result


# sql_query = "SELECT * FROM table1 JOIN (SELECT * FROM table2) subquery ON table1.id = subquery.id"
def remove_comments_gu(sql):
    print("remove_comments")
    # 한 줄 주석 제거
    if sql.strip().startswith('#') or sql.strip().startswith('//'):
        sql = re.sub(r''
                     r'^#.*$', '', sql, flags=re.MULTILINE)
        #다음 구문의 문제점은 , '#' A1_SVC_AGRMT_ID   같은 것도 지운다
        # sql = re.sub(r''
        #              r'#.*$', '', sql, flags=re.MULTILINE)
    # Remove single-line comments (--)
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)

    # Remove multi-line comments (/* */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

    return sql


def remove_comments(file):
    filtered_lines = []

    for line in file:
        if line.strip().startswith('#') or line.strip().startswith('--'):
            continue
        line = re.sub(r'--.*$', '', line)

        # Remove multi-line comments (/* */)
        line = re.sub(r'/\*.*?\*/', '', line)
        #print("###line2", line)
        filtered_lines.append(line)

    return '\n'.join(filtered_lines)





# This code is borrowed from sqlparse example script.
# <url>
def is_subselect(parsed):
    # print('parsed.is_group', parsed, parsed.is_group)
    if not parsed.is_group:
        return False
    if parsed.is_group:
        # if parsed in "(":
        #     parsed = parsed.replace("(", "").replace(")", "")
        # print("parsed.is_group", parsed)
        for item in parsed.tokens:
            if item.ttype is not sqlparse.tokens.Whitespace:
                # print("item22", item)
                pass

        # pass
        # if item.ttype is not sqlparse.tokens.Whitespace:
        # print('parsed.tokens', parsed.tokens)
    for item in parsed.tokens:
        if item.ttype is not sqlparse.tokens.Whitespace:
            pass
            # print ("item.ttype is not sqlparse.tokens.Whitespace," , item.value)
        if item.ttype is not None and item.ttype is not sqlparse.tokens.Whitespace:
            # print('parsed.tokens', parsed.tokens)
            # print("item.ttype", item.ttype, item.value.upper())
            if (
                item.ttype is DML
                and item.value.upper() in ("SELECT", "INSERT", "UPDATE", "CREATE", "DELETE")
            ):
                # print('kkk: ', item)
                return True
    return False


def _identifier_is_function(identifier):
    return any(isinstance(t, Function) for t in identifier.tokens)


def extract_from_part(parsed, stop_at_punctuation=True):
    tbl_prefix_seen = False
    for item in parsed.tokens:
        if tbl_prefix_seen:
            # print("item tbl_prefix_seen", item, tbl_prefix_seen)
            if is_subselect(item):
                # print("item 서브쿠리", item)
                for x in extract_from_part(item, stop_at_punctuation):
                    yield x
            elif stop_at_punctuation and item.ttype is Punctuation:
                raise StopIteration
            # An incomplete nested select won't be recognized correctly as a
            # sub-select. eg: 'SELECT * FROM (SELECT id FROM user'. This causes
            # the second FROM to trigger this elif condition resulting in a
            # StopIteration. So we need to ignore the keyword if the keyword
            # FROM.
            # Also 'SELECT * FROM abc JOIN def' will trigger this elif
            # condition. So we need to ignore the keyword JOIN and its variants
            # INNER JOIN, FULL OUTER JOIN, etc.
            elif item.ttype is Keyword and (not item.value.upper() == "FROM") and (
                not item.value.upper().endswith("JOIN")
            ):
                tbl_prefix_seen = False
            else:
                yield item
        elif item.ttype is Keyword or item.ttype is Keyword.DML:
            item_val = item.value.upper()
            if (
                item_val in ("COPY", "FROM", "INTO", "UPDATE", "TABLE")
                or item_val.endswith("JOIN")
            ):
                tbl_prefix_seen = True
        # 'SELECT a, FROM abc' will detect FROM as part of the column list.
        # So this check here is necessary.
        elif isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                if identifier.ttype is Keyword and identifier.value.upper() == "FROM":
                    tbl_prefix_seen = True
                    break


def extract_table_identifiers(token_stream, allow_functions=True):
    """yields tuples of TableReference namedtuples"""

    def parse_identifier(item):
        name = item.get_real_name()
        schema_name = item.get_parent_name()
        alias = item.get_alias()
        if not name:
            schema_name = None
            name = item.get_name()
            alias = alias or name
        schema_quoted = schema_name and item.value[0] == '"'
        if schema_name and not schema_quoted :
            schema_name = schema_name.lower()
        quote_count = item.value.count('"')
        name_quoted = quote_count > 2 or (quote_count and not schema_quoted)
        alias_quoted = alias and item.value[-1] == '"'
        if alias_quoted or name_quoted and not alias and name.islower():
            alias = '"' + (alias or name) + '"'
        if name and not name_quoted and not name.islower():
            if not alias:
                alias = name
            name = name.lower()
        return schema_name, name, alias

    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                # Sometimes Keywords (such as FROM ) are classified as
                # identifiers which don't have the get_real_name() method.
                try:
                    schema_name = identifier.get_parent_name()
                    real_name = identifier.get_real_name()
                    is_function = (
                        allow_functions and _identifier_is_function(identifier)
                    )
                except AttributeError:
                    continue
                if real_name:
                    yield TableReference(
                        schema_name, real_name, identifier.get_alias(), is_function
                    )
        elif isinstance(item, Identifier):
            schema_name, real_name, alias = parse_identifier(item)
            is_function = allow_functions and _identifier_is_function(item)

            yield TableReference(schema_name, real_name, alias, is_function)
        elif isinstance(item, Function):
            schema_name, real_name, alias = parse_identifier(item)
            yield TableReference(None, real_name, alias, allow_functions)


# extract_tables is inspired from examples in the sqlparse lib.
def extract_tables(sql):
    """Extract the table names from an SQL statment.

    Returns a list of TableReference namedtuples

    """
    parsed = sqlparse.parse(sql)
    if not parsed:
        return ()

    # INSERT statements must stop looking for tables at the sign of first
    # Punctuation. eg: INSERT INTO abc (col1, col2) VALUES (1, 2)
    # abc is the table name, but if we don't stop at the first lparen, then
    # we'll identify abc, col1 and col2 as table names.
    insert_stmt = parsed[0].token_first().value.lower() == "insert"
    stream = extract_from_part(parsed[0], stop_at_punctuation=insert_stmt)
    # print("stream", stream)

    # Kludge: sqlparse mistakenly identifies insert statements as
    # function calls due to the parenthesized column list, e.g. interprets
    # "insert into foo (bar, baz)" as a function call to foo with arguments
    # (bar, baz). So don't allow any identifiers in insert statements
    # to have is_function=True
    identifiers = extract_table_identifiers(stream, allow_functions=not insert_stmt)
    # In the case 'sche.<cursor>', we get an empty TableReference; remove that
    return tuple(i for i in identifiers if i.name)

def extract_tables_exp(sql):
    # Extract table names using regular expressions
#    table_names = re.findall(r'\bFROM\s+([\w.]+)', sql, re.IGNORECASE)
#    table_names += re.findall(r'\bJOIN\s+([\w.]+)', sql, re.IGNORECASE)
    data = {}  # 딕셔너리생성
    # 테이블 목록 추출
    table_names = re.findall(r'\bFROM\s+([\w.]+)', sql, re.IGNORECASE)

    # INSERT 문의 테이블 추출
    insert_list = []
    #특수문자 제거시
    #pattern = re.compile(r"INSERT\s+OVERWRITE\s+TABLE\s+\$\{([^}]+)\}\.\w+")  # re.compile( r"\{(.+?)\}[\.|\/](\w*)")   # re.compile( r"\{(.+?)\}.(\w*)")                # re.compile(r"\{(.+?)\}.(\w*)")
    pattern = re.compile(r"(?:OVERWRITE\s+|INTO\s+)TABLE\s+\w+\.\w+")  # re.compile( r"\{(.+?)\}[\.|\/](\w*)")   # re.compile( r"\{(.+?)\}.(\w*)")                # re.compile(r"\{(.+?)\}.(\w*)")

    if re.search(pattern, sql):
        matches = pattern.findall(sql)
        matches = matches[0].split(' ')[-1]
        #insert_list =  re.findall(r"INSERT\s+OVERWRITE\s+TABLE\s+\$\{([^}]+)\}\.\w+", sql)
        data = {"INSERT":matches}
        print("###insert_list", data)

    if insert_list:
        insert_list.append(insert_list.group(1))

    for insert in insert_list:
        print(f"insert_list-{insert[0]}: {insert[1]}")

    # Print the table names
    if table_names:
        print(f"Table names: {table_names}")
    else:
        print("No table names found.")
    return table_names




def remove_spe(sql):
    sql = sql.replace('${', '').replace('}', '')
    return sql


if __name__ == '__main__':

    sql_query = """
                INSERT OVERWRITE TABLE ${DB_TMP}.F_STORDSVC_SALE_SELR_TMP PARTITION (PT_SUB='1')
                INSERT INTO TABLE ${TMP}.F_STORDSVC_SALE_SELR_TMP PARTITION (PT_SUB='1')            
                SELECT A.SVC_MGMT_NUM
                     , NVL(CONCAT(C.ORG_CD, C.SUB_ORG_CD),'#')    AS SELR_POST_ORG_ID   -- 신청판매점조직ID
                     , A.SVC_SCRB_DT      
                  FROM ${TMP}.F_STORDSVC_SALE_MOSU_TMP A
                  LEFT OUTER JOIN
                       (SELECT B1.SVC_MGMT_NUM
                             , MAX(B1.REQ_SALE_BR_ORG_ID) AS MAX_REQ_SALE_BR_ORG_ID
                             , MAX(B1.RCV_SEQ)            AS MAX_RCV_SEQ
                          FROM ${SWG}.NMORD_WIRE_SVC_RCV  B1                               --(T)유선서비스접수
                         WHERE B1.SVC_CHG_CD       = 'A1'
                           AND B1.SVC_CHG_RSN_CD  <> '06'
                           AND B1.DEL_FLAG         = 'N'
                         GROUP BY B1.SVC_MGMT_NUM
                         UNION ALL
                        SELECT B1.SVC_MGMT_NUM
                             , MAX(B1.REQ_SALE_BR_ORG_ID) AS MAX_REQ_SALE_BR_ORG_ID
                             , MAX(B1.RCV_SEQ)            AS MAX_RCV_SEQ
                          FROM ${DB_SWG}.RSORD_WIRE_SVC_RCV  B1                               --(T)유선서비스접수
                         WHERE B1.SVC_CHG_CD       = 'A1'
                           AND B1.SVC_CHG_RSN_CD  <> '06'
                           AND B1.DEL_FLAG         = 'N'
                         GROUP BY B1.SVC_MGMT_NUM
                       ) B
                    ON A.SVC_MGMT_NUM = B.SVC_MGMT_NUM
                  LEFT OUTER JOIN ${DB_SWG}.NMNGM_ORG C 
                    ON B.MAX_REQ_SALE_BR_ORG_ID = C.ORG_ID
                   AND C.DEL_FLAG = 'N'
                ;
             """


    sql_query = remove_comments(sql_query)
    sql_query = remove_spe(sql_query)

    table_list = extract_tables_exp(sql_query)
    #print("table_list", table_list)

    print("#############")


    # tables = ', '.join(extract_tables(sql_query))
    tables = extract_tables(sql_query)
    print('Tables: {}'.format(tables))
    #print("type" , type(tables))


    for table_reference in tables:
        schema = table_reference.schema
        table_name = table_reference.name
        table_list.append(f"{schema}.{table_name}")




    print("#############합친후")
    #print("table_list2", table_list)
    # 대문자로 변환하고 중복을 제거하는 방법
    table_list2_upper = list(set([table.upper() for table in table_list]))

    schema_list = ["BMT","SWG","TMP","SMS","BCR","CDR","ACT","BCM","ACC","ACO","BDWSOR","RSLT","ACP","ADH","ABL","TAS","TBMT","RTN",]
    schema_exists_list = []
    schema_not_exists_list = []


    for table in table_list2_upper:
        table_schema = table.split('.')[0]
        if table_schema in schema_list:
            schema_exists_list.append(table)
            #print(f"{table} - Schema exis