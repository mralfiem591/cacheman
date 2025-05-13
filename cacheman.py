import sqlite3
import threading
import time
from os import path

class Cacheman:
    def __init__(self, db_path, path_mode, data_type='dict', backup_interval=600, allow_data_loss=True, allow_loading=True):
        """
        Initialize the Cacheman instance.

        :param db_path: Path to the SQLite database file.
        :param path_mode: Mode for the path ('absolute' or 'relative').
        :param data_type: Type of data storage ('dict' or 'list').
        :param backup_interval: Interval in seconds for periodic backups.
        """
        if path_mode == 'absolute':
            self.db_path = db_path
        elif path_mode == 'relative':
            self.db_path = path.join(path.dirname(__file__), db_path)
        else:
            raise ValueError("Invalid path mode. Use 'absolute' or 'relative'.")
        self.data_type = data_type
        self.backup_interval = backup_interval
        self.allow_data_loss = allow_data_loss
        self.allow_loading = allow_loading
        self._lock = threading.Lock()

        if data_type == 'dict':
            self._data = {}
        elif data_type == 'list':
            self._data = []
        else:
            raise ValueError("Unsupported data type. Use 'dict' or 'list'.")
        if path.exists(self.db_path) and allow_loading:
            self.load_from_db()
        else:
            self._initialize_db()
        self._start_backup_thread()
        print(f"Cacheman initialized with data type '{data_type}' and backup interval of {backup_interval} seconds.")

    def _initialize_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            conn.commit()

    def _start_backup_thread(self):
        if not self.allow_data_loss:
            print("Periodic backup is disabled due to allow_data_loss being False. Backups are made at other points.")
            return
        thread = threading.Thread(target=self._backup_periodically, daemon=True)
        thread.start()

    def _backup_periodically(self):
        while True:
            time.sleep(self.backup_interval)
            self.backup_to_db()

    def add(self, key, value):
        if self.get(key) is not None:
            return KeyError(f"Key '{key}' already exists.")
        with self._lock:
            if self.data_type == 'dict':
                self._data[key] = value
            elif self.data_type == 'list':
                self._data.append((key, value))
        self.backup_to_db()

    def get(self, key):
        with self._lock:
            if self.data_type == 'dict':
                return self._data.get(key) or None
            elif self.data_type == 'list':
                for k, v in self._data:
                    if k == key:
                        return v
                return None

    def remove(self, key):
        with self._lock:
            if self.data_type == 'dict':
                if key in self._data:
                    del self._data[key]
            elif self.data_type == 'list':
                self._data = [(k, v) for k, v in self._data if k != key]
        self.backup_to_db()

    def edit(self, key, new_value):
        with self._lock:
            if self.data_type == 'dict':
                if key in self._data:
                    self._data[key] = new_value
            elif self.data_type == 'list':
                self._data = [(k, new_value) if k == key else (k, v) for k, v in self._data]
        self.backup_to_db()


    def search(self, query):
        with self._lock:
            if self.data_type == 'dict':
                return {k: v for k, v in self._data.items() if query in k or query in str(v)}
            elif self.data_type == 'list':
                return [(k, v) for k, v in self._data if query in k or query in str(v)]
            
    def get_all(self):
        with self._lock:
            if self.data_type == 'dict':
                return self._data.copy()
            elif self.data_type == 'list':
                return self._data.copy()

    def backup_to_db(self):
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if self.data_type == 'dict':
                    for key, value in self._data.items():
                        cursor.execute('''
                            INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)
                        ''', (key, value))
                elif self.data_type == 'list':
                    for key, value in self._data:
                        cursor.execute('''
                            INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)
                        ''', (key, value))
                conn.commit()

    def load_from_db(self):
        if not self.allow_loading:
            raise RuntimeError("Loading from DB is not allowed.")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT key, value FROM cache
            ''')
            rows = cursor.fetchall()

        with self._lock:
            if self.data_type == 'dict':
                self._data = {key: value for key, value in rows}
            elif self.data_type == 'list':
                self._data = rows
