import sqlite3
import json
from datetime import datetime

timeframe ='2006-05'
sql_transaction =[]

conn = sqlite3.connect('{}.db'.format(timeframe))
c = conn.cursor()

def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reps
    (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,parent TEXT,
    comment TEXT, subreddit TEXT,unix INT, score INT)""")


def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM parent_reps WHERE comment_id='{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        value = c.fetchone()
        if value != None:
            return value[0]
        else:
            return False
    except Exception as e:
        return False


def format_body(data):
    data = data.replace("\n","  newlinechar  ").replace("\r","  newlinechar  ").replace('"',"'")
    return data

def accept(data):
    if len(data.split('  '))>50 or len(data) < 1:
        return False
    elif len(data)>1000:
        return False
    elif data=="[deleted]" or data=="[removed]":
        return False
    else:
        return True

def sql_insert_replace(commentid,parentid,parentdata,data,subreddits,createdutc,scoredata):
    try:
        sql = """UPDATE parent_reps SET comment_id = commentid, parent_id = parentid , parent = parentdata , comment=data , subreddit=subreddits, unix=createdutc , score=scoredata WHERE parent_id=parentid;"""
        transaction_add(sql);
    except Exception as e:
        print("replace-error",e)

def find_score(pid):
        try:
            sql = "SELECT score FROM parent_reps WHERE parent_id='{}' LIMIT 1".format(pid)
            c.execute(sql)
            value = c.fetchone()
            if value != None:
                return value[0]
            else:
                return False
        except Exception as e:
            return False


def sql_insert_parent(commentid,parentid,parentdata,body,subreddit,createdutc,score):
    try:
        sql = """INSERT INTO parent_reps (parent_id,comment_id,parent,comment,subreddit,unix,score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid,commentid,parentdata,body,subreddit,createdutc,score)
        transaction_add(sql);
    except Exception as e:
        print(e)

def sql_insert(commentid,parentid,body,subreddit,createdutc,score):
    try:
        sql = """INSERT INTO parent_reps (parent_id,comment_id,comment,subreddit,unix,score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid,commentid,body,subreddit,createdutc,score)
        transaction_add(sql);
    except Exception as e:
        print(e)

def transaction_add(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        conn.commit()
        sql_transaction=[]

if __name__=="__main__":
    create_table()
    row_counter = 0
    pair_counter = 0

    with open("RC_2015-05".format(timeframe),buffering=1000) as f:
        for rows in f:
            row_counter = row_counter+1
            rows = json.loads(rows)
            parent_id = rows['parent_id']
            body = format_body(rows['body'])
            created_utc = rows['created_utc']
            comment_id = rows['name']
            score = rows['score']
            subreddit = rows['subreddit']
            parent_data = find_parent(parent_id)


            if score > 0:
                if accept(body):
                    existing_comment = find_score(parent_id)
                    if existing_comment:
                        if score > existing_comment:
                            sql_insert_replace(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                    else:
                        if parent_data:
                            sql_insert_parent(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                        else:
                            sql_insert(comment_id,parent_id,body,subreddit,created_utc,score)
            print(row_counter)
