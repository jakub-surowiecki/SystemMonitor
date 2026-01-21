from flask import Flask, render_template, request, redirect, url_for, session, flash
from functools import wraps
import sqlite3
import os
import psutil
import json
from core.database import DatabaseManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES = {"blacklist": "blacklist.txt", "monitored_list.txt": "monitored_list.txt"}
CONFIG_FILE = "config.json"
IGNORED_UI = {
    "svchost.exe", "conhost.exe", "dllhost.exe", "taskhostw.exe", "System",
    "Registry", "smss.exe", "csrss.exe", "winlogon.exe", "services.exe",
    "lsass.exe", "fontdrvhost.exe", "Memory Compression", "spoolsv.exe"
}

app = Flask(__name__)
app.secret_key = 'zmien_to_na_trudny_losowy_klucz'

db_manager = DatabaseManager()


def get_db():
    conn = sqlite3.connect(db_manager.db_name)
    conn.row_factory = sqlite3.Row
    return conn


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            flash('Musisz się zalogować, aby wykonać tę akcję.', 'danger')
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if db_manager.verify_user(username, password):
            session['logged_in'] = True
            session['user'] = username
            flash('Zalogowano pomyślnie.', 'success')
            return redirect(url_for('index'))

        return render_template('login.html', error='Błędny login lub hasło.')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('Wylogowano.', 'info')
    return redirect(url_for('index'))


@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    current_user = session.get('user', 'admin')

    if request.method == 'POST' and 'action_security' in request.form:
        new_user = request.form.get('new_username')
        new_pass = request.form.get('new_password')
        confirm_pass = request.form.get('confirm_password')

        if new_pass and new_pass == confirm_pass and new_user:
            if db_manager.update_credentials(current_user, new_user, new_pass):
                session['user'] = new_user
                flash(f'Zaktualizowano dane. Witaj {new_user}!', 'success')
            else:
                flash('Błąd: Taki użytkownik może już istnieć.', 'danger')
        else:
            flash('Hasła nie pasują lub pola są puste.', 'danger')
        return redirect(url_for('settings'))

    if request.method == 'POST' and 'action_config' in request.form:
        config_data = {}
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config_data = json.load(f)
            except:
                pass

        for key, fname in {"blacklist": "blacklist.txt", "monitored": "monitored_list.txt"}.items():
            content = request.form.get(key)
            if content:
                content = "\n".join([line.strip() for line in content.splitlines() if line.strip()])
                with open(fname, 'w', encoding='utf-8') as f: f.write(content)

        new_email = request.form.get('receiver_email')
        if new_email and 'email' in config_data:
            config_data['email']['receiver_email'] = new_email.strip()
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config_data, f, indent=4)

        flash('Ustawienia monitoringu zapisane.', 'success')
        return redirect(url_for('settings'))

    config_data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config_data = json.load(f)
        except:
            pass

    content = {}
    for key, fname in FILES.items():
        if os.path.exists(fname):
            with open(fname, 'r', encoding='utf-8') as f: content[key] = f.read()

    current_email = config_data.get('email', {}).get('receiver_email', '')

    return render_template('settings.html',
                           current_email=current_email,
                           current_user=current_user,
                           **content)


@app.route('/kill_process', methods=['POST'])
@login_required
def kill_process():
    target = request.form.get('target_name')
    if target:
        conn = get_db()
        conn.execute("INSERT INTO pending_commands (command, target) VALUES ('KILL', ?)", (target,))
        conn.commit()
        conn.close()
        flash(f'⚠️ Wysłano rozkaz zakończenia: {target}', 'warning')
    return redirect(url_for('index'))


@app.route('/')
def index():
    is_admin = session.get('logged_in', False)

    conn = get_db()
    c = conn.cursor()

    usage_data = c.execute(
        "SELECT process_name, SUM(duration_seconds) as total_seconds FROM process_usage_stats GROUP BY process_name ORDER BY total_seconds DESC LIMIT 5").fetchall()
    blocked_logs = c.execute(
        "SELECT process_name, reason, strftime('%Y-%m-%d %H:%M:%S', added_at) as added_at FROM blocked_processes ORDER BY added_at DESC LIMIT 10").fetchall()
    blocks_today = c.execute("SELECT COUNT(*) FROM blocked_processes WHERE date(added_at) = date('now')").fetchone()[0]
    total_time = c.execute("SELECT SUM(duration_seconds) FROM process_usage_stats").fetchone()[0] or 0
    total_hours = round(total_time / 3600, 1)

    hourly_raw = c.execute(
        "SELECT strftime('%H', start_time) as hour, COUNT(*) as count FROM process_usage_stats WHERE start_time IS NOT NULL GROUP BY hour ORDER BY hour").fetchall()
    hours_map = {row['hour']: row['count'] for row in hourly_raw}
    hourly_labels = [f"{i:02d}:00" for i in range(24)]
    hourly_data = [hours_map.get(f"{i:02d}", 0) for i in range(24)]

    top_blocked = c.execute(
        "SELECT process_name, COUNT(*) as count FROM blocked_processes GROUP BY process_name ORDER BY count DESC LIMIT 5").fetchall()
    conn.close()

    running_processes = []
    try:
        for p in psutil.process_iter(['pid', 'name', 'memory_info', 'cpu_percent']):
            try:
                if p.info['name'] and p.info['name'] not in IGNORED_UI:
                    cpu_count = psutil.cpu_count()
                    raw_cpu = p.info['cpu_percent'] if p.info['cpu_percent'] is not None else 0.0
                    cpu = round(raw_cpu / cpu_count, 1)
                    running_processes.append({
                        'pid': p.info['pid'],
                        'name': p.info['name'],
                        'mem': round(p.info['memory_info'].rss / 1048576, 1),
                        'cpu': cpu
                    })
            except:
                pass
    except:
        pass
    running_processes.sort(key=lambda x: x['mem'], reverse=True)

    # NOWE STATYSTYKI
    process_count = len(running_processes)
    system_memory = psutil.virtual_memory().percent

    return render_template('dashboard.html',
                           is_admin=is_admin,
                           labels=[r['process_name'] for r in usage_data],
                           data=[r['total_seconds'] for r in usage_data],
                           blocked=blocked_logs,
                           blocks_today=blocks_today,
                           total_hours=total_hours,
                           live=running_processes[:50],
                           process_count=process_count,  # NOWE
                           system_memory=system_memory,  # NOWE
                           hourly_labels=hourly_labels,
                           hourly_data=hourly_data,
                           top_blocked_labels=[r['process_name'] for r in top_blocked],
                           top_blocked_data=[r['count'] for r in top_blocked])


if __name__ == '__main__':
    app.run(debug=True, port=5000)