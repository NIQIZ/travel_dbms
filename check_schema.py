import sqlite3

conn = sqlite3.connect('travel.sqlite')
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("=== DATABASE TABLES ===")
for table in tables:
    print(f"\n{table[0]}")
    
    # Get column info for each table
    cursor.execute(f"PRAGMA table_info({table[0]})")
    columns = cursor.fetchall()
    
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cursor.fetchone()[0]
    print(f"  Total rows: {count}")

conn.close()