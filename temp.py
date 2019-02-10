import sqlite3


if __name__ == '__main__':
    conn = sqlite3.connect('{}.db'.format('2006-05'))
    c = conn.cursor()
    sql = """SELECT * FROM parent_reps  WHERE parent NOT NULL;"""
    c.execute(sql)
    row = c.fetchall()
    for r in row:
        print(r)
        print("\n")
