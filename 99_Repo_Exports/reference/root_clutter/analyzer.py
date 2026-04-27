import psycopg2
from psycopg2 import sql
import sys

try:
    conn = psycopg2.connect("postgresql://postgres:12345@localhost:5434/trade")
    cur = conn.cursor()

    # Имена столбцов — через параметр %s (безопасно, это значение, а не идентификатор)
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        ("signals",),
    )
    columns = [row[0] for row in cur.fetchall()]

    if not columns:
        print("Table 'signals' not found or has no columns.")
        sys.exit(0)

    cur.execute("SELECT count(*) FROM signals")
    total_count = cur.fetchone()[0]
    print(f"Total rows in 'signals' table: {total_count}")

    if total_count == 0:
        print("Table is empty.")
        sys.exit(0)

    print(f"{'Column':<30} | {'Null Count':<15} | {'Null %':<10} | {'Empty Str Count (if text)'}")
    print("-" * 85)

    for col in columns:
        # sql.Identifier безопасно экранирует имя столбца (защита от инъекций через имена)
        cur.execute(
            sql.SQL("SELECT count(*) FROM signals WHERE {} IS NULL").format(
                sql.Identifier(col)
            )
        )
        null_count = cur.fetchone()[0]
        null_pct = (null_count / total_count) * 100

        # Тип столбца — передаём table_name и column_name как значения (%s)
        cur.execute(
            "SELECT data_type FROM information_schema.columns"
            " WHERE table_name = %s AND column_name = %s",
            ("signals", col),
        )
        data_type = cur.fetchone()[0]

        empty_str_count = "N/A"
        if data_type in ["character varying", "text", "character"]:
            cur.execute(
                sql.SQL("SELECT count(*) FROM signals WHERE {} = ''").format(
                    sql.Identifier(col)
                )
            )
            empty_str_count = cur.fetchone()[0]
            if empty_str_count > 0:
                empty_str_count = f"{empty_str_count} ({(empty_str_count/total_count)*100:.2f}%)"
            else:
                empty_str_count = "0"

        print(f"{col:<30} | {null_count:<15} | {null_pct:>6.2f}%   | {empty_str_count}")

    conn.close()
except Exception as e:
    print(f"Error: {e}")
