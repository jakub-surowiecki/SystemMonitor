from datetime import datetime
from core.logger import logger

class UsageTracker:
    def __init__(self):
        self.active = {}

    def start(self, name):
        if name not in self.active:
            self.active[name] = datetime.now()

    def stop(self, name):
        if name in self.active:
            start_time = self.active.pop(name)
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds())
            logger.info(f"[TRACKER] Koniec {name}: {duration}s")
            return name, start_time, end_time, duration
        return None