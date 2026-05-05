from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor


# main function to connect to database:

# this will open the connection
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn= hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


# this will close the connection
def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()


# Creating Schemas
def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


# Creating Table
def create_table(schema, table):
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" int,
                    "Likes_Count" int,
                    "Comment_Count" int);
                    """
    else:
        table_sql =  f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" int,
                    "Likes_Count" int,
                    "Comment_Count" int);
                    """
        
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ID = [row["Video_ID"] for row in ids]
    return video_ID


