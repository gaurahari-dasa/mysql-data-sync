import pymysql

def get_foreign_keys(conn, table_name):
    cursor = conn.cursor()
    query = f"""
        SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = '{table_name}'
          AND REFERENCED_TABLE_NAME IS NOT NULL
    """
    cursor.execute(query)
    results = cursor.fetchall()
    foreign_keys = {}
    for col, ref_table, ref_col in results:
        foreign_keys[col] = (ref_table, ref_col)
    return foreign_keys
