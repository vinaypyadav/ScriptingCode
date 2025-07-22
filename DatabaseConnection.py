import psycopg2

def get_postgres_connection(
    # host="localhost",
    # port=5432,
    # dbname="mdm_play",
    # user="postgres",
    # password="bitxia"
   #dev db

    host="172.236.189.244",   
    port=5433,
    dbname="mdm_play",
    user="dbadmin",
    password="Tajub#riw^75"
):
    """
    Returns a PostgreSQL connection object.
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("✅ Connected to PostgreSQL DB.")
        return conn
    except psycopg2.Error as e:
        print("❌ Error connecting to DB:", e)
        return None

# # Example usage:
# if __name__ == "__main__":
#     conn = get_postgres_connection()
#     if conn:
#         conn.close()