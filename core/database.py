import sqlite3
import socket
import getpass
import json
from datetime import datetime
from core.logger import logger


class DatabaseManager:
    def __init__(self, config_file="config.json"):
        self.db_name = "system_monitor.db"
        self._load_config(config_file)
        self._init_db()

    def _load_config(self, config_file):
        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                self.db_name = data["database"]["file_name"]
        except:
            pass

    def _get_conn(self):
        return sqlite3.connect(self.db_name, check_same_thread=False)

    def _init_db(self):
        """Tworzy tabele, jeśli nie istnieją."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Tabela monitorowania
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS process_usage_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process_name TEXT NOT NULL,
                start_time DATETIME,
                end_time DATETIME,
                duration_seconds INTEGER,
                hostname TEXT, username TEXT
            )
        """)
        # Tabela blokad
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blocked_processes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process_name TEXT NOT NULL,
                reason TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Tabela zasobów (CPU/RAM)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resource_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process_name TEXT,
                cpu_percent REAL,
                memory_mb REAL,
                captured_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Tabela komend (Remote Kill)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT,
                target TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def save_usage(self, process_name, start, end, duration):
        try:
            conn = self._get_conn()
            conn.execute("""
                INSERT INTO process_usage_stats (process_name, start_time, end_time, duration_seconds, hostname, username)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (process_name, start, end, duration, socket.gethostname(), getpass.getuser()))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"DB Error (Usage): {e}")

    def log_block(self, process_name, reason="Blacklist"):
        try:
            conn = self._get_conn()
            conn.execute("INSERT INTO blocked_processes (process_name, reason, added_at) VALUES (?, ?, ?)",
                         (process_name, reason, datetime.now()))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"DB Error (Block): {e}")

    def log_resource_usage(self, data_list):
        if not data_list: return
        try:
            conn = self._get_conn()
            conn.executemany("""
                INSERT INTO resource_stats (process_name, cpu_percent, memory_mb, captured_at)
                VALUES (?, ?, ?, ?)
            """, data_list)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"DB Error (Resources): {e}")

    def get_pending_commands(self):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT id, command, target FROM pending_commands WHERE status='PENDING'")
            rows = cursor.fetchall()
            conn.close()
            return rows
        except:
            return []

    def mark_command_executed(self, cmd_id):
        try:
            conn = self._get_conn()
            conn.execute("UPDATE pending_commands SET status='EXECUTED' WHERE id=?", (cmd_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"DB Error (Command): {e}")