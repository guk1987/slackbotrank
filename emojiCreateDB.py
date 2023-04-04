import sqlite3

conn = sqlite3.connect("slack.db")
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS emoji_usage
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                item_user_id TEXT,
                timestamp TEXT,
                reaction TEXT,
                event_type TEXT)''')
conn.commit()
conn.close()