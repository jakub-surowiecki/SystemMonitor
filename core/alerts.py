import smtplib
import json
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from core.logger import logger


class EmailAlert:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = ""
        self.sender_pass = ""
        self.receiver = ""

        # Pierwsze ładowanie
        self._load_config()

    def _load_config(self):
        """Ładuje konfigurację (wywoływane przed każdym wysłaniem, żeby łapać zmiany)."""
        try:
            with open(self.config_file, "r") as f:
                data = json.load(f)["email"]
                self.smtp_server = data.get("smtp_server", "smtp.gmail.com")
                self.smtp_port = data.get("smtp_port", 587)
                self.sender_email = data.get("sender_email", "")
                self.sender_pass = data.get("sender_password", "")
                self.receiver = data.get("receiver_email", "")
        except:
            pass

    def _send(self, subject, body):
        # Zawsze odświeżamy konfig przed wysłaniem
        self._load_config()

        if not self.sender_email or not self.sender_pass or not self.receiver:
            return

        try:
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"] = self.sender_email
            msg["To"] = self.receiver
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_pass)
                server.send_message(msg)
            logger.info(f"[EMAIL] Wysłano: {subject} -> {self.receiver}")
        except Exception as e:
            logger.error(f"Błąd wysyłania email: {e}")

    def send_block_alert(self, process_name):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        host = socket.gethostname()
        subject = f"[BLOKADA] Wykryto: {process_name}"
        body = (f"SYSTEM MONITORINGU\n\n"
                f"Zablokowano proces: {process_name}\n"
                f"Data: {now}\n"
                f"Komputer: {host}\n\n"
                f"Proces został natychmiast zamknięty.")
        self._send(subject, body)

    def send_start_alert(self, process_name):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        host = socket.gethostname()
        subject = f"[INFO] Uruchomiono: {process_name}"
        body = (f"SYSTEM MONITORINGU\n\n"
                f"Wykryto start aplikacji monitorowanej: {process_name}\n"
                f"Data: {now}\n"
                f"Komputer: {host}")
        self._send(subject, body)

    def send_stop_alert(self, process_name, start_time, duration):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_str = str(start_time).split('.')[0]
        subject = f"[KONIEC] Zamknięto: {process_name}"
        body = (f"SYSTEM MONITORINGU\n\n"
                f"Zakończono proces: {process_name}\n"
                f"Czas pracy: {duration} sekund\n"
                f"Start: {start_str}\n"
                f"Koniec: {now}")
        self._send(subject, body)