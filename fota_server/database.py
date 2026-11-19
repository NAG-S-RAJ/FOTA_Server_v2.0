import psycopg2

from . import config as settings, state

conn = None
cursor = None


def connect_db():
    global conn, cursor

    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = True
    cursor = conn.cursor()

    state.add_log("[DB][CONNECTED] database connection established")


def ensure_connection():
    global conn, cursor

    try:
        if conn is None or conn.closed:
            connect_db()
            return

        if cursor is None or cursor.closed:
            cursor = conn.cursor()
            return

        cursor.execute("SELECT 1")

    except Exception as e:
        state.add_log(f"[DB][RECONNECT] connection lost ({type(e).__name__}: {e}); reconnecting")
        connect_db()


def migrate_spaak_schema(database_cursor):
    columns = (
        ("registered_tbms", "bcm_version", "spaak_version"),
        ("campaigns", "bcm_target_version", "spaak_target_version"),
        ("campaigns", "bcm_firmware", "spaak_firmware")
    )
    for table, previous, current in columns:
        database_cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema=current_schema() AND table_name=%s
              AND column_name IN (%s, %s)
            """,
            (table, previous, current)
        )
        existing = {row[0] for row in database_cursor.fetchall()}
        if previous in existing and current not in existing:
            database_cursor.execute(f"ALTER TABLE {table} RENAME COLUMN {previous} TO {current}")
        elif previous in existing and current in existing:
            database_cursor.execute(f"UPDATE {table} SET {current}=COALESCE({current}, {previous})")
    database_cursor.execute(
        "UPDATE campaigns SET target_ecu=REPLACE(target_ecu, %s, %s) WHERE target_ecu LIKE %s",
        ("BCM", "SPAAK", "%BCM%")
    )


def initialize_database():
    connect_db()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS registered_tbms (

        vin VARCHAR(50) PRIMARY KEY,

        sgw_version VARCHAR(50),

        spaak_version VARCHAR(50),

        added_on TIMESTAMPTZ

    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaigns(

        campaign_id VARCHAR(100) PRIMARY KEY,

        vin VARCHAR(50),

        target_ecu VARCHAR(20),

        campaign_name VARCHAR(200),

        sgw_target_version VARCHAR(50),

        spaak_target_version VARCHAR(50),

        sgw_firmware VARCHAR(255),

        spaak_firmware VARCHAR(255),

        status VARCHAR(50),

        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP

    )
    """)

    cursor.execute("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ")

    cursor.execute("ALTER TABLE campaigns ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP")

    migrate_spaak_schema(cursor)

    for column, definition in (
        ("execution_started_at", "DOUBLE PRECISION"),
        ("last_heartbeat_at", "DOUBLE PRECISION"),
        ("last_progress_at", "DOUBLE PRECISION"),
        ("failed_at", "DOUBLE PRECISION"),
        ("error_code", "VARCHAR(64)"),
        ("error_message", "TEXT"),
        ("max_retries", "INTEGER DEFAULT 0"),
        ("retry_count", "INTEGER DEFAULT 0"),
        ("completed_at", "DOUBLE PRECISION"),
        ("initiation", "VARCHAR(20) DEFAULT 'immediate'"),
        ("scheduled_at", "DOUBLE PRECISION"),
        ("description", "TEXT")
    ):
        cursor.execute(f"ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS {column} {definition}")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS campaign_events(
        id SERIAL PRIMARY KEY,
        campaign_id VARCHAR(100),
        vin VARCHAR(50),
        event_type VARCHAR(40),
        status VARCHAR(50),
        detail TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS campaign_events_campaign_idx ON campaign_events (campaign_id, id)")

    conn.commit()

    state.add_log("[DB][SCHEMA_READY] tables and migrations verified")


def close_database():
    global conn, cursor
    try:
        if cursor is not None and not cursor.closed:
            cursor.close()
    finally:
        if conn is not None and not conn.closed:
            conn.close()
        cursor = None
        conn = None
