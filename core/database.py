import sqlite3
import threading
import bcrypt
from core.logger import logger


class DatabaseManager:
    def __init__(self, db_name="system_monitor.db"):
        self.db_name = db_name
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_name, check_same_thread=False)

    def _init_db(self):
        with self._lock:
            conn = self._get_conn()
            c = conn.cursor()

            c.execute('''CREATE TABLE IF NOT EXISTS blocked_processes
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          process_name TEXT,
                          added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          reason TEXT)''')

            c.execute('''CREATE TABLE IF NOT EXISTS process_usage_stats
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          process_name TEXT,
                          start_time TIMESTAMP,
                          end_time TIMESTAMP,
                          duration_seconds REAL)''')

            c.execute('''CREATE TABLE IF NOT EXISTS pending_commands
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          command TEXT,
                          target TEXT,
                          status TEXT DEFAULT 'PENDING',
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

            c.execute('''CREATE TABLE IF NOT EXISTS resource_usage
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          process_name TEXT,
                          cpu_percent REAL,
                          memory_mb REAL,
                          timestamp TIMESTAMP)''')

            # Tabela użytkowników
            c.execute('''CREATE TABLE IF NOT EXISTS users
                         (username TEXT PRIMARY KEY,
                          password_hash BLOB)''')

            # Domyślny admin (admin / admin)
            c.execute("SELECT * FROM users WHERE username = 'admin'")
            if not c.fetchone():
                default_pass = "admin"
                hashed = bcrypt.hashpw(default_pass.encode('utf-8'), bcrypt.gensalt())
                c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", ('admin', hashed))
                logger.warning("Utworzono domyślnego użytkownika: admin / admin")

            conn.commit()
            conn.close()

    def verify_user(self, username, password):
        with self._lock:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
            row = c.fetchone()
            conn.close()

            if row:
                return bcrypt.checkpw(password.encode('utf-8'), row[0])
            return False

    def update_credentials(self, current_user, new_user, new_password):
        new_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    "UPDATE users SET username = ?, password_hash = ? WHERE username = ?",
                    (new_user, new_hash, current_user)
                )
                conn.commit()
                logger.info(f"Zaktualizowano dane logowania: {current_user} -> {new_user}")
                return True
            except sqlite3.IntegrityError:
                return False
            finally:
                conn.close()

    def log_block(self, process_name):
        with self._lock:
            conn = self._get_conn()
            conn.execute("INSERT INTO blocked_processes (process_name, reason) VALUES (?, ?)",
                         (process_name, "Blacklist match"))
            conn.commit()
            conn.close()

    def save_usage(self, name, start, end, duration):
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO process_usage_stats (process_name, start_time, end_time, duration_seconds) VALUES (?, ?, ?, ?)",
                (name, start, end, duration))
            conn.commit()
            conn.close()

    def get_pending_commands(self):
        with self._lock:
            conn = self._get_conn()
            c = conn.cursor()
            cmds = c.execute("SELECT id, command, target FROM pending_commands WHERE status='PENDING'").fetchall()
            conn.close()
            return cmds

    def mark_command_executed(self, cid):
        with self._lock:
            conn = self._get_conn()
            conn.execute("UPDATE pending_commands SET status='EXECUTED' WHERE id=?", (cid,))
            conn.commit()
            conn.close()

    def log_resource_usage(self, data_list):
        if not data_list: return
        with self._lock:
            conn = self._get_conn()
            conn.executemany(
                "INSERT INTO resource_usage (process_name, cpu_percent, memory_mb, timestamp) VALUES (?, ?, ?, ?)",
                data_list)
            conn.commit()
            conn.close()