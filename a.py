import os, psycopg

url = os.environ["DATABASE_URL"]

with psycopg.connect(url) as conn:
    conn.execute("DELETE FROM login_users WHERE COALESCE(payload->>'username','') = ''")
    conn.commit()

print("done")
