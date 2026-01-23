# System Monitor and Dashboard

Aplikacja systemowa przeznaczona do monitorowania aktywności procesów w systemie Windows. Program działa jako usługi systemowe, oferuje zaawansowane statystyki użycia zasobów, automatyczne blokowanie aplikacji oraz zdalne zarządzanie procesami przez interfejs WWW.

## Główne funkcjonalności
* **Praca w tle:** System działa jako dwie niezależne usługi Windows (Backend i Dashboard).
* **Monitoring WMI:** Wykrywanie startu i zakończenia procesów w czasie rzeczywistym.
* **System Bezpieczeństwa:** Panel administracyjny zabezpieczony logowaniem (hashowanie bcrypt).
* **Zarządzanie Kontem:** Możliwość zmiany nazwy użytkownika oraz hasła z poziomu ustawień.
* **Tryb Gościa:** Dashboard dostępny w trybie tylko do odczytu (statystyki), funkcje administracyjne (Zabij proces, Ustawienia) wymagają logowania.
* **Monitoring Zasobów:** Śledzenie użycia CPU i RAM dla poszczególnych procesów oraz ogólnego stanu pamięci systemu.
* **Filtrowanie:** Wyszukiwarka procesów na żywo oraz przycisk odświeżania danych.
* **Alerty:** Powiadomienia e-mail o wykryciu lub zablokowaniu aplikacji zdefiniowanych na listach.

## Główne Statystyki
* **Czas pracy:** Łączny czas monitorowania procesów.
* **Blokady dzisiaj:** Liczba prób uruchomienia zabronionych aplikacji w bieżącym dniu.
* **Procesy:** Aktualna liczba działających procesów w systemie.
* **RAM (System):** Procentowe zużycie pamięci operacyjnej całego komputera.

## Konfiguracja powiadomień E-mail
Aby system mógł wysyłać alerty, należy poprawnie skonfigurować plik `config.json` znajdujący się w głównym folderze aplikacji.

**Przykładowa struktura pliku:**
```json
{
    "email": {
        "sender_email": "twoj_mail@gmail.com",
        "sender_password": "twoje_haslo_aplikacji",
        "receiver_email": "adres_docelowy@gmail.com",
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587
    }
}
```

> **Ważne:** W przypadku Gmaila, jako `sender_password` należy użyć specjalnie wygenerowanego **"Hasła aplikacji"**, a nie głównego hasła do konta. Hasło to można wygenerować w ustawieniach bezpieczeństwa konta Google (wymagana włączona weryfikacja dwuetapowa).

## Instalacja
1. Pobierz instalator `SystemMonitor.exe` z sekcji **Releases**.
2. Uruchom plik jako **Administrator**.
3. Po zakończeniu instalacji panel jest dostępny pod adresem: http://localhost:5000.
4. Domyślne dane logowania: `admin` / `admin`. Zaleca się natychmiastową zmianę danych w zakładce Ustawienia.

## Bezpieczeństwo danych
* Hasła są przechowywane w bazie SQLite w formie bezpiecznych hashy (bcrypt).
* Plik `config.json` oraz baza `system_monitor.db` powinny być chronione i nie udostępniane publicznie.
