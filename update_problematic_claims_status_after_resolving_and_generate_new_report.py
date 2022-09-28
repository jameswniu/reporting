import os, sys
import re

import psycopg2

from datetime import datetime as dt
from pytz import timezone as tz

from config import user_db, passwd_db
from database__reporting__automation__store_alerts_automatically_cross_verify_status_w_other_tables_create_report_and_format_it import backup_jopari_responses, generate_james_report, format_macro, refresh_billed_status


def update_responses_flag(file, x, con):
    YmdHM = dt.now(tz=tz('America/New_York')).strftime('%Y%m%d')
    Ymd = YmdHM[:8]

    print('#' * 150)
    print('sql_update_manual_processed...')
    print('#' * 150)
    sql = """\
update
    business_analysis.jopari_claim_alerts
set
    processed = 't'
    , processed_at = '{}'::date
where
    pm_sk = {}
    and processed = 'f';"""

    fr = open(file, 'r')

    for line in fr:
        if re.match(r'\d{7}\b', line):
            ky = line.strip()
            # print(ky)

            sq1 = sql.format(Ymd, ky)

            cur = con.cursor()
            cur.execute(sq1)

            rowcount = cur.rowcount

            cur.close()

            if rowcount != 0:
                print(sq1)

            x += rowcount

    fr.close()

    return x


def output_james_report(conn):
    backup_jopari_responses(conn)
    generate_james_report(conn)

    return format_macro()


def main():
    os.chdir(r'D:\Users\james.niu\Desktop')

    c0 = 0
    c1 = 0

    params = {
        'host': 'revpgdb01.revintel.net',
        'database': 'tpliq_tracker_db',
        'user': user_db,
        'password': passwd_db}
    conn = psycopg2.connect(**params)

    with conn:
        a = refresh_billed_status(c0, conn)
        print('\n')
        b = update_responses_flag('new1.txt', c1, conn)    # SPECIFY

        #----------------------------------------------------------------------------------------------------------
        # Run
        #----------------------------------------------------------------------------------------------------------
        print('#' * 150 + f'\nupdated billing processed {a}...updated manual processed {b}...')
        # os.system(f'start excel.exe {output_james_report(conn)}')   # off = default, on = see new report
        #----------------------------------------------------------------------------------------------------------

        conn.commit()

    conn.close()


main()

