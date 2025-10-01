#!/usr/bin/env python3
"""
live_daily_attendance.py

Live poller for ZKTeco K40 that:
- creates attendance_YYYY-MM-DD.csv each day (all students Absent at start),
- marks a student Present (and records first_checkin_local) the first time their fingerprint is seen that day,
- prints live events to console,
- avoids duplicate marking within the same day,
- persists processed device epochs & daily presents in a small sqlite DB so restarts are safe.

Configure DEVICE_IP and, if desired, POLL_INTERVAL and ROLLOVER_HOUR below.
Requires: pip install pyzk
"""
import os, time, csv, sqlite3, sys
from datetime import datetime, date, timedelta

try:
    from zk import ZK
except Exception:
    print("Missing pyzk library. Install with: pip install pyzk")
    sys.exit(1)

# ---------- CONFIG ----------
DEVICE_IP = '192.168.1.201'     # <-- set your device IP here
PORT = 4370
USERS_CSV = 'users.csv'         # id,name
OUTPUT_DIR = '.'                # where attendance files are written
POLL_INTERVAL = 3               # seconds between polls (small for near-instant)
ROLLOVER_HOUR = 0               # local hour when day rolls over (0 = midnight)
DB_PATH = 'attendance_state.db' # sqlite state
# ---------- END CONFIG ----------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS processed (device_epoch INTEGER PRIMARY KEY)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS present (day TEXT, user_id INTEGER, first_checkin_local TEXT,
                   PRIMARY KEY(day,user_id))''')
    conn.commit(); conn.close()

def load_users():
    if not os.path.exists(USERS_CSV):
        raise SystemExit(f"Missing {USERS_CSV}. Create it with header 'id,name'.")
    users = {}
    with open(USERS_CSV, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            uid = int(row['id'])
            users[uid] = {'id': uid, 'name': row['name']}
    return users

def get_meta(k):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (k,))
    r = cur.fetchone(); conn.close()
    return r[0] if r else None

def set_meta(k,v):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", (k,str(v)))
    conn.commit(); conn.close()

def mark_epoch_processed(epoch):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO processed (device_epoch) VALUES (?)", (int(epoch),))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()

def is_epoch_processed(epoch):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed WHERE device_epoch=? LIMIT 1", (int(epoch),))
    r = cur.fetchone(); conn.close()
    return bool(r)

def day_str_for_dt(dt):
    return dt.date().isoformat()

def is_user_marked_today(user_id, daystr):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT first_checkin_local FROM present WHERE day=? AND user_id=? LIMIT 1", (daystr, user_id))
    r = cur.fetchone(); conn.close()
    return r[0] if r else None

def mark_user_present(user_id, daystr, first_checkin_local):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO present (day,user_id,first_checkin_local) VALUES (?,?,?)",
                (daystr, user_id, first_checkin_local))
    conn.commit(); conn.close()

def make_daily_csv(users, day):
    filename = os.path.join(OUTPUT_DIR, f"attendance_{day}.csv")
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id','name','status','first_checkin_local'])
        for uid in sorted(users):
            w.writerow([uid, users[uid]['name'], 'Absent', ''])
    set_meta('current_csv_date', day)
    print(f"[{datetime.now()}] Created {filename}")
    return filename

def update_csv_mark_present(filename, uid, checkin_str):
    tmp_rows = []
    updated = False
    with open(filename, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for rr in r:
            if int(rr['id']) == uid:
                if rr['status'] != 'Present':
                    rr['status'] = 'Present'
                    rr['first_checkin_local'] = checkin_str
                    updated = True
            tmp_rows.append(rr)
    if updated:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(['id','name','status','first_checkin_local'])
            for rr in tmp_rows:
                w.writerow([rr['id'], rr['name'], rr['status'], rr['first_checkin_local']])
    return updated

def device_epoch_to_local_str(epoch):
    # epoch is seconds
    dt = datetime.fromtimestamp(int(epoch))
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def poll_once(users, csvfile):
    zk = ZK(DEVICE_IP, port=PORT, timeout=5)
    conn = None
    try:
        conn = zk.connect()
        recs = conn.get_attendance()  # list of records with .user_id and .timestamp
        # sort ascending by timestamp (older first)
        recs = sorted(recs, key=lambda r: r.timestamp)
        for rec in recs:
            epoch = int(rec.timestamp)
            if is_epoch_processed(epoch):
                continue
            uid = rec.user_id
            # mark processed always so we don't reprocess old logs
            mark_epoch_processed(epoch)

            # validate user exists locally
            if uid not in users:
                print(f"[{datetime.now()}] Device record for unknown user {uid} at {device_epoch_to_local_str(epoch)} - skipping.")
                continue

            today = day_str_for_dt(datetime.fromtimestamp(epoch))
            # Check if user already present today
            existing = is_user_marked_today(uid, today)
            if existing:
                # already present today; ignore
                print(f"[{datetime.now()}] {users[uid]['name']} ({uid}) scanned again at {device_epoch_to_local_str(epoch)} - already marked Present today ({existing}).")
                continue

            # Mark present
            checkin_str = device_epoch_to_local_str(epoch)
            marked = update_csv_mark_present(csvfile, uid, checkin_str)
            mark_user_present(uid, today, checkin_str)
            print(f"[{datetime.now()}] MARKED PRESENT: {users[uid]['name']} ({uid}) at {checkin_str}  -> CSV updated: {os.path.basename(csvfile)}")
    except Exception as e:
        print(f"[{datetime.now()}] Poll error: {e}")
    finally:
        try:
            if conn: conn.disconnect()
        except:
            pass

def ensure_csv_for_today(users):
    now = datetime.now()
    # active day depends on ROLLOVER_HOUR
    if now.hour < ROLLOVER_HOUR:
        active_day = (now - timedelta(days=1)).date()
    else:
        active_day = now.date()
    daystr = active_day.isoformat()
    current = get_meta('current_csv_date')
    csvfile = os.path.join(OUTPUT_DIR, f"attendance_{daystr}.csv")
    if current != daystr or not os.path.exists(csvfile):
        # create fresh csv and clear any 'present' rows for the new day (present table is per-day so okay)
        make_daily_csv(users, daystr)
    return csvfile, daystr

def main_loop():
    init_db()
    users = load_users()
    csvfile, daystr = ensure_csv_for_today(users)
    print(f"[{datetime.now()}] Live poller started. Poll interval {POLL_INTERVAL}s. Current day: {daystr}")
    try:
        while True:
            # check rollover (midnight or ROLLOVER_HOUR)
            csvfile, newday = ensure_csv_for_today(users)
            if newday != daystr:
                # day changed
                daystr = newday
                # optionally reload users.csv in case names changed
                users = load_users()
                print(f"[{datetime.now()}] New day started: {daystr}. CSV: {csvfile}")

            poll_once(users, csvfile)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping poller (Ctrl+C).")

if __name__ == '__main__':
    main_loop()
