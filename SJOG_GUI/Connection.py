

def init_db_connection(connect_to_db, get_all_table_primary_keys, CONNECTION_STRING, log_fn):
    log_fn("⏳ Connecting to database...")
    try:
        conn, cursor = connect_to_db(CONNECTION_STRING)
        table_dict, _ = get_all_table_primary_keys(cursor)
        log_fn("✅ Database connection established.")
        return conn, cursor, table_dict
    except Exception as e:
        log_fn(f"❌ Failed to connect to database: {e}")
        return None, None, None