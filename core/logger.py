import logging
import json
import os

def setup_logger():
    """Konfiguruje logger zapisujący zdarzenia do pliku i konsoli."""
    log_file = "system_monitor.log"

    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
                log_file = config.get("files", {}).get("log_file", log_file)
        except:
            pass

    logger = logging.getLogger("SystemMonitor")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S')

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()