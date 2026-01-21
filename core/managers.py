import os
import json
import subprocess
from core.logger import logger

class FileManager:
    """Klasa bazowa do obsługi list w plikach tekstowych z auto-odświeżaniem."""
    def __init__(self, file_key, default_file, config_file="config.json"):
        self.file_path = default_file
        self.items = set()
        self.last_mtime = 0
        self._load_config(config_file, file_key)
        self._refresh()

    def _load_config(self, config_file, key):
        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                self.file_path = data["files"].get(key, self.file_path)
        except:
            pass

    def _refresh(self):
        if not os.path.exists(self.file_path):
            return
        try:
            mtime = os.path.getmtime(self.file_path)
            if mtime > self.last_mtime:
                with open(self.file_path, "r", encoding='utf-8') as f:
                    self.items = set(line.strip().lower() for line in f if line.strip())
                self.last_mtime = mtime
                logger.info(f"Odświeżono {self.file_path}: {len(self.items)} pozycji.")
        except Exception as e:
            logger.error(f"Błąd odczytu {self.file_path}: {e}")

    def contains(self, name):
        self._refresh()
        return name.lower() in self.items

class BlacklistManager(FileManager):
    def __init__(self):
        super().__init__("blacklist", "blacklist.txt")

    def kill_process(self, process_name):
        """Zabija proces poleceniem systemowym."""
        try:
            cmd = f"taskkill /F /IM {process_name}"
            subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True # Zakładamy sukces dla uproszczenia logiki flow
        except Exception as e:
            logger.error(f"Błąd taskkill {process_name}: {e}")
            return False

class MonitoredManager(FileManager):
    def __init__(self):
        super().__init__("monitored", "monitored_list.txt")