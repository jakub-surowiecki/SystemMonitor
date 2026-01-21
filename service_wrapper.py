import win32serviceutil
import win32service
import win32event
import servicemanager
import sys
import os
import threading
import main  # Importujemy Twój plik main.py
from core.logger import logger


class MonitorBackendSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "MonitorSystemuBackend"
    _svc_display_name_ = "Monitor Systemu - Backend (Ochrona)"
    _svc_description_ = "Monitoruje procesy, blokuje aplikacje i zbiera statystyki."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_event = threading.Event()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        logger.info("Usługa Backend: Otrzymano sygnał STOP.")
        self.stop_event.set()  # Sygnał dla pętli w main.py
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        # Fix ścieżek dla EXE
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(base_path)

        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))

        # Uruchomienie logiki z main.py
        try:
            main.run(self.stop_event)
        except Exception as e:
            logger.critical(f"Błąd krytyczny usługi Backend: {e}")


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MonitorBackendSvc)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MonitorBackendSvc)