import time
import sys
import os
import psutil
from datetime import datetime

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

os.chdir(BASE_DIR)
sys.path.append(BASE_DIR)

from core.logger import logger
from core.database import DatabaseManager
from core.managers import BlacklistManager, MonitoredManager
from core.alerts import EmailAlert
from core.tracker import UsageTracker
from core.monitor import ProcessMonitor

IGNORED = {
    "svchost.exe", "conhost.exe", "csrss.exe", "winlogon.exe", "services.exe",
    "lsass.exe", "System", "Registry", "smss.exe", "Idle", "System Idle Process",
    "Memory Compression", "fontdrvhost.exe", "spoolsv.exe", "taskhostw.exe"
}
ALWAYS_LOG = {"chrome.exe", "firefox.exe", "msedge.exe", "opera.exe", "explorer.exe"}

db = DatabaseManager()
blacklist = BlacklistManager()
monitored = MonitoredManager()
alerts = EmailAlert()
tracker = UsageTracker()


def on_start(name):
    if name in IGNORED: return
    try:
        # 1. BLOKADA
        if blacklist.contains(name):
            logger.warning(f"[BLOKADA] Wykryto: {name}")
            if blacklist.kill_process(name):
                db.log_block(name)
                if monitored.contains(name):
                    # ZMIANA: Konkretna metoda alertu
                    alerts.send_block_alert(name)
            return

        # 2. MONITORING
        if name not in ALWAYS_LOG and name in tracker.active: return

        if monitored.contains(name):
            # ZMIANA: Konkretna metoda alertu
            alerts.send_start_alert(name)

        tracker.start(name)
    except Exception as e:
        logger.error(f"Err Start {name}: {e}")


def on_stop(name):
    if name in IGNORED: return
    try:
        res = tracker.stop(name)
        if res:
            db.save_usage(*res)
            # p_name, start, end, duration = res
            if monitored.contains(name):
                # ZMIANA: Konkretna metoda alertu (przekazujemy start i duration)
                alerts.send_stop_alert(name, res[1], res[3])
    except Exception as e:
        logger.error(f"Err Stop {name}: {e}")


def check_remote_commands():
    cmds = db.get_pending_commands()
    for cid, cmd, target in cmds:
        if cmd == 'KILL':
            logger.warning(f"[REMOTE] Zabijanie zdalne: {target}")
            blacklist.kill_process(target)
            db.mark_command_executed(cid)


def monitor_resources():
    data = []
    now = datetime.now()
    try:
        for p in psutil.process_iter(['name', 'cpu_percent', 'memory_info']):
            try:
                name = p.info['name']
                if name and (monitored.contains(name) or name in tracker.active):
                    mem = round(p.info['memory_info'].rss / 1048576, 2)
                    cpu = p.info['cpu_percent']
                    if mem > 1:
                        data.append((name, cpu, mem, now))
            except:
                pass
    except:
        pass
    db.log_resource_usage(data)


def run(stop_event=None):
    monitor = ProcessMonitor(on_start, on_stop)
    monitor.start()

    logger.info(f"--- SERWIS (WMI) URUCHOMIONY W: {os.getcwd()} ---")

    tick = 0
    try:
        while True:
            if stop_event and stop_event.is_set(): break
            time.sleep(1)
            tick += 1
            check_remote_commands()
            if tick % 5 == 0: monitor_resources()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"Main loop error: {e}")
    finally:
        monitor.stop()
        logger.info("Zatrzymano serwis.")


if __name__ == "__main__":
    run()