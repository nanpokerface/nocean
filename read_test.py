import re
 
# SAMPLE_TEST.py 파일 읽기 (인코딩: utf-8)
with open("SAMPLE_TEST.py", "r", encoding="utf-8") as file:
    code = file.read()

# SQL 문 추출
sql_statements = re.findall(r"spark\.sql\(\"(.*?)\"\)", code, re.DOTALL)
#sql_statements = re.findall(r"spark\.sql\(f\"\"\"([\s\S]*?)\"\"\"\)", code)

# 데이터프레임 추출
#dataframe_pattern = r"(\w+)\s*= \\\s*spark\.sql\(f\"\"\"([\s\S]*?)\"\"\"\)"
#dataframes = re.findall(dataframe_pattern, code, re.DOTALL)

#dataframe_pattern = r"table_\w+"   # ['table_df1', 'table_df2', 'table_df3', 'table_df', 'table_df1', 'table_df', 'table_df2', 'table_df', 'table_df3', 'table_df']
#dataframe_pattern = r"(\w+)table_\w+"   # ['all_', 'all_', 'all_', 'all_']
dataframe_pattern = r"(df_table[1-9]+)\s*=\s"   # ['table_df1', 'table_df2', 'table_df3']
dataframes = re.findall(dataframe_pattern, code, re.DOTALL)
print("dataframes@@@",dataframes)

# 추출된 데이터프레임 출력
# for df_name in dataframes:
#     print(f"데이터프레임 - {df_name}:")
#     print()

# 결과 출력
# print("추출된 SQL 문:")
# for i, sql_statement in enumerate(sql_statements):
#     #formatted_sql = sql_statement.replace("\n", " ").strip()
#     print(f"SQL {i+1}:")
#     print(sql_statement)
#     print()

dataframe_mapping = {}

lines = code.split("\n")
line_number = 1

for line in lines:
    for dataframe in dataframes:
        if line.startswith(dataframe):
            dataframe_mapping[dataframe] = {
                "sql_statement": sql_statements[dataframes.index(dataframe)],
                "start_line": line_number
            }
    line_number += 1

# 데이터프레임, SQL 문, 시작하는 행 라인 번호 매핑 출력
for dataframe, info in dataframe_mapping.items():
    print(f"Start Line: {info['start_line']}")
    print(f"Dataframe: {dataframe}")
    print(f"SQL Statement: {info['sql_statement']}")
    print()



print("################")


save_dataframe_mapping = {}

# pattern = r"(\w+)\.write\s*\\"
# pattern = r"(\w+)\.write\s*\\(.*)\)"
save_start_pattern = r"(\w+)\.write\s*\\(.*)\)"
save_end_pattern = r"save\(\"s3a:\/\/[\w\-]+\/([\w\/\-]+)"
start_match = re.finditer(save_start_pattern, code, re.DOTALL)
end_match = re.search(save_end_pattern, code)
print("matches", start_match)
for match in start_match:
    save_dataframe_name = match.group(1)
    start_index = match.start()
    end_index = end_match.end()
    save_extracted_string = code[start_index:end_index]


    print("save_dataframe_name", start_index, end_index, save_dataframe_name)
    print("save_extracted_string", save_extracted_string)

    # save_path_match = re.search(r"save\(\"([\w\/\.\-]+)\"\)", code[end_index:])
    # save_path_match = re.search(       r"save\(\"s3a:\/\/([\w\-]+)\/([\w\/\-]+)\"", code[end_index:])
    save_path_match =   re.search(r"save\(\"s3a:\/\/[\w\-]+\/([\w\/\-]+)", code[end_index:])
    if save_path_match:
        save_path = save_path_match.group(1)
        save_dataframe_mapping[save_dataframe_name] = save_path
        print("save_path",save_path)
        print()

# # # 데이터프레임명과 SAVE 경로 매핑 출력
# for dataframe_name, save_path in dataframe_mapping.items():
#     print(f"Dataframe Name: {dataframe_name}")
#     print(f"Save Path: {save_path}")
#     print()



# DataFrame �