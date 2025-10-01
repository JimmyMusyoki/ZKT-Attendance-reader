# fetch_attendance.py
from zk import ZK
import csv
from datetime import datetime

DEVICE_IP = '192.168.1.201'   # change this to your K40 IP
PORT = 4370

def fetch_attendance(ip, port=4370):
    zk = ZK(ip, port=port, timeout=8)
    conn = zk.connect()
    attendance = conn.get_attendance()   # list of Attendance objects
    conn.disconnect()
    return attendance

def load_users(filename='users.csv'):
    users = {}
    try:
        with open(filename, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                users[row['id']] = row['name']
    except FileNotFoundError:
        print("⚠ users.csv not found. Names will not be mapped.")
    return users

def write_attendance_csv(attendance, users, filename='attendance.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id','name','timestamp','status'])
        for att in attendance:
            uid = str(att.user_id)
            name = users.get(uid, 'Unknown')
            timestamp = att.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            status = att.status  # often 0 = check-in, 1 = check-out
            writer.writerow([uid, name, timestamp, status])
    print(f"✅ Wrote {len(attendance)} attendance records to {filename}")

if __name__ == '__main__':
    users = load_users('users.csv')
    attendance = fetch_attendance(DEVICE_IP, PORT)
    write_attendance_csv(attendance, users)
