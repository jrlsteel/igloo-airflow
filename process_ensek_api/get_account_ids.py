import pymysql

import sys
import os
from connections import connect_db as db

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conf import config as con


def get_accountID_fromDB(get_max):

    conn = db.get_rds_connection()
    cur = conn.cursor()
    cur.execute(con.test_config['account_ids_sql'])
    account_ids = [row[0] for row in cur]

    # logic to get max external id and process all the id within them
    if get_max:
        account_ids = list(range(1, max(account_ids)+1))
        # account_ids = list(range(max(account_ids)-10, max(account_ids)+1))

    db.close_rds_connection(cur, conn)

    return account_ids

