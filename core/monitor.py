import threading
import pythoncom  # <--- To jest kluczowe dla usług Windows
import win32com.client
from core.logger import logger


class ProcessMonitor:
    def __init__(self, on_start, on_stop):
        self.on_start = on_start
        self.on_stop = on_stop
        self.running = False

    def _loop(self):
        # WAZNE: W nowym wątku usługi musimy ręcznie inicjować COM
        pythoncom.CoInitialize()

        try:
            logger.info("Inicjalizacja WMI Monitor...")
            wmi = win32com.client.Dispatch("WbemScripting.SWbemLocator").ConnectServer(".", "root\\CIMV2")

            # Nasłuchujemy startu (interwał 1s)
            start_watcher = wmi.ExecNotificationQuery(
                "SELECT * FROM __InstanceCreationEvent WITHIN 1 WHERE TargetInstance ISA 'Win32_Process'"
            )

            # Nasłuchujemy stopu (interwał 1s)
            stop_watcher = wmi.ExecNotificationQuery(
                "SELECT * FROM __InstanceDeletionEvent WITHIN 1 WHERE TargetInstance ISA 'Win32_Process'"
            )

            logger.info("WMI Monitor (Events) uruchomiony pomyślnie.")

            while self.running:
                try:
                    # Sprawdzamy zdarzenie START
                    try:
                        # Czekamy 100ms na zdarzenie, żeby nie blokować pętli na amen
                        event = start_watcher.NextEvent(100)
                        proc_name = event.TargetInstance.Name
                        self.on_start(proc_name)
                    except pythoncom.com_error as e:
                        # Błąd timeoutu (0x80041032) jest normalny, gdy nic się nie dzieje
                        if e.hresult != -2147217358:
                            raise e

                    # Sprawdzamy zdarzenie STOP
                    try:
                        event = stop_watcher.NextEvent(100)
                        proc_name = event.TargetInstance.Name
                        self.on_stop(proc_name)
                    except pythoncom.com_error as e:
                        if e.hresult != -2147217358:
                            raise e

                except Exception as e:
                    # Ignorujemy błędy timeoutu, logujemy inne
                    pass

        except Exception as e:
            logger.critical(f"Krytyczny błąd WMI: {e}")
        finally:
            # Sprzątamy po COM
            pythoncom.CoUninitialize()
            logger.info("WMI Monitor zatrzymany.")

    def start(self):
        self.running = True
        # Daemon=True oznacza, że wątek zginie razem z głównym programem
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.running = False