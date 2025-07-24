import pymysql
import json
from infer_foreign_keys import get_foreign_keys

def fetch_rows(conn, table_name, pk_column):
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()
    return {row[pk_column]: row for row in rows}

def generate_insert_sql(table_name, row):
    columns = ', '.join(f"`{col}`" for col in row.keys())
    values = ', '.join(
        f"'{str(val).replace("'", "\\'")}'" if val is not None else 'NULL'
        for val in row.values()
    )
    return f"INSERT INTO `{table_name}` ({columns}) VALUES ({values});"

def resolve_dependencies(source_conn, target_conn, table_name, pk_column, visited):
    if table_name in visited:
        return [], []

    visited.add(table_name)

    foreign_keys = get_foreign_keys(source_conn, table_name)
    insert_statements = []
    missing_rows = []

    for fk_col, (ref_table, ref_col) in foreign_keys.items():
        ref_missing, ref_inserts = resolve_dependencies(source_conn, target_conn, ref_table, ref_col, visited)
        insert_statements.extend(ref_inserts)
        missing_rows.extend(ref_missing)

    source_rows = fetch_rows(source_conn, table_name, pk_column)
    target_rows = fetch_rows(target_conn, table_name, pk_column)

    for pk, row in source_rows.items():
        if pk not in target_rows:
            insert_statements.append(generate_insert_sql(table_name, row))
            missing_rows.append(row)

    return missing_rows, insert_statements

def main():
    with open("config.json") as f:
        config = json.load(f)

    source_conn = pymysql.connect(**config["source_db"])
    target_conn = pymysql.connect(**config["target_db"])
    table_name = config["table_name"]
    pk_column = config["primary_key"]

    missing_rows, insert_sqls = resolve_dependencies(source_conn, target_conn, table_name, pk_column, set())

    print("Missing rows in target DB:")
    for row in missing_rows:
        print(row)
    print("\nInsert statements:")
    for stmt in insert_sqls:
        print(stmt)

if __name__ == "__main__":
    main()
