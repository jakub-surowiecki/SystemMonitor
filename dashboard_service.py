import win32serviceutil
import win32service
import win32event
import servicemanager
import sys
import os
from waitress import serve
from dashboard import app  # Importujemy Twoją aplikację Flask
from core.logger import logger


class MonitorDashboardSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "MonitorSystemuDashboard"
    _svc_display_name_ = "Monitor Systemu - Dashboard WWW"
    _svc_description_ = "Serwer WWW dla statystyk i ustawień (Port 5000)."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        logger.info("Usługa Dashboard: Zatrzymywanie...")
        win32event.SetEvent(self.hWaitStop)
        # Waitress nie ma eleganckiego 'stop' w wątku głównym,
        # więc zabicie procesu przez SCM jest standardem.

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

        try:
            # Uruchamiamy profesjonalny serwer na porcie 5000
            serve(app, host='0.0.0.0', port=5000, threads=4)
        except Exception as e:
            logger.critical(f"Błąd krytyczny usługi Dashboard: {e}")


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MonitorDashboardSvc)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MonitorDashboardSvc)