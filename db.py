import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host     = os.getenv("DB_HOST"),
        port     = os.getenv("DB_PORT", 5432),
        dbname   = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )

def get_or_create_user(phone: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE phone = %s", (phone,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO users (phone) VALUES (%s) RETURNING id", (phone,))
        return cur.fetchone()[0]
    

def get_thread_by_user_and_assistant(user_id: int, assistant_key: str) -> dict | None:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, openai_thread_id
            FROM threads
            WHERE user_id = %s AND assistant_key = %s
            ORDER BY created_at DESC
            LIMIT 1
            """, (user_id, assistant_key)
        )
        return cur.fetchone()

def create_thread_db(user_id: int, assistant_key: str, openai_thread_id: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO threads (user_id, assistant_key, openai_thread_id)
            VALUES (%s, %s, %s) RETURNING id
            """, (user_id, assistant_key, openai_thread_id)
        )
        return cur.fetchone()[0]


def log_message(thread_db_id: int, role: str, content: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO messages (thread_id, role, content)
            VALUES (%s, %s, %s)
            """, (thread_db_id, role, content)
        )


def fetch_history(thread_db_id: int) -> list[dict]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT role, content, sent_at
            FROM messages
            WHERE thread_id = %s
            ORDER BY sent_at
            """, (thread_db_id,)
        )
        return cur.fetchall()