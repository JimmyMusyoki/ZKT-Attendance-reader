# export_users_from_k40.py
from zk import ZK
import csv

DEVICE_IP = '192.168.1.201'   # <-- change this to your K40 IP
PORT = 4370

def fetch_users(ip, port=4370):
    zk = ZK(ip, port=port, timeout=8)
    conn = zk.connect()
    users = conn.get_users()
    conn.disconnect()
    return users

def write_users_csv(users, filename='users.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id','name'])
        for u in users:
            uid = getattr(u, 'uid', None) or getattr(u, 'user_id', None)
            name = getattr(u, 'name', '') or ''
            if isinstance(name, bytes):
                try:
                    name = name.decode('utf-8')
                except:
                    name = name.decode('latin1', errors='ignore')
            w.writerow([int(uid), name])
    print(f"Wrote {len(users)} users to {filename}")

if __name__ == '__main__':
    users = fetch_users(DEVICE_IP, PORT)
    write_users_csv(users)
