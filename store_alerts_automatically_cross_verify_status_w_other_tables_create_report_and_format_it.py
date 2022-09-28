import os, sys
import re

import psycopg2
import openpyxl
import xlwings as xw

from glob import glob
from datetime import datetime as dt
from pytz import timezone as tz
from xlwings import Book

from config import user_db, passwd_db


def refresh_billed_status(x, con):
    print('#' * 150)
    print('sql_update_billing_processed...')
    print('#' * 150)
    sql_update_processed = """\
with cte0 as (
select max(cust_id) cust_id, max(pat_acct) pat_acct, pm_sk, max(created_at)::date bill_date 
from tpl_billing_records group by pm_sk
)
-->
update
    business_analysis.jopari_claim_alerts a
set
    processed = true
    , processed_at = b.bill_date
from
    cte0 b
where a.pm_sk = b.pm_sk and a.effective_date <= b.bill_date and processed = false;"""
    cur = con.cursor()
    cur.execute(sql_update_processed)
    print(sql_update_processed)

    x += cur.rowcount

    cur.close()

    return x


def update_jopari_responses(file, x, con):
    ymd_file = re.search(r'\d+', file).group()

    print('#' * 150)
    print('sql_populate_update_frm_billing...')
    print('#' * 150)
    sql = """\
insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), {}, {}, '{}', '{}', '{}'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = {}
        and src_sk = {}
        and pat_acct = '{}'
        and note = '{}'
        and code = '{}');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '{}'::date
      , assigned = '{}'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = {}
      and a.processed is null;"""

    with open(file, 'r') as fr, open('update_' + ymd_file + '_responses.sql', 'w') as fw:
        for line in fr:
            if not re.match(r'\s+\d{7}', line):
                assign = line.strip()
                continue
            # print(assign)

            nums = [w.strip() for w in line.split('-')[0].split(',')]
            msg = re.search(r'-.*', line).group().replace('-', '').strip()
            msg = re.sub(r'\s+>\s+', ' - ', msg)
            code = '{}{}'.format('277-', re.search(r'.*(?= -)', msg).group())
            # print(nums)
            # print(msg)
            # print(code)

            sql1 = sql.format(nums[0], nums[1], nums[2], msg, code
                            , nums[0], nums[1], nums[2], msg, code
                            , ymd_file, assign, nums[0])
            print(sql1, file=fw)
            print(file=fw)


    raw_sql = open('update_' + ymd_file + '_responses.sql', 'r').read()
    sql_list = raw_sql.split(';')[:-1]
    # print(sql_list[-1].strip())
    # print(sql_list[-2].strip())
    # [print(x) for x in sql_list]

    a = 0
    b = 0

    for sql in sql_list:
        cur = con.cursor()
        cur.execute(sql)
        print(sql)

        if a % 2 == 0:
            b += cur.rowcount

        a += 1

        cur.close()

    print()
    print()
    print('#' * 150)
    print('sql_update_frm_prebilling...')
    print('#' * 150)
    sql_update_frm_pbilling = """\
update 
    business_analysis.jopari_claim_alerts a 
set 
    claim_type = b.content->>'claim_type'
    , vx_carrier_lob = b.content->>'vx_carrier_lob'
    , work_comp_flag = nullif(b.content->>'work_comp_flag', '')
from 
    tpl_pre_billing_records b 
where 
    a.pm_sk = b.pm_sk
    and a.created_at::date = now()::date;"""
    cur = con.cursor()
    cur.execute(sql_update_frm_pbilling)

    cur.close()

    print(sql_update_frm_pbilling)

    print()
    print()
    c = refresh_billed_status(x, con)

    print('#' * 150)
    print('inserted {}... updated billing processed {}...'.format(b, c))


def backup_jopari_responses(con):
    sql_backup = """\
drop table if exists business_analysis.jopari_claim_alerts_backup;
create table if not exists business_analysis.jopari_claim_alerts_backup as (
select * from business_analysis.jopari_claim_alerts
);"""

    cur = con.cursor()
    cur.execute(sql_backup)

    # cur.close()

    print('\nbacked up to CP...')


def generate_james_report(con):
    YmdHM = dt.now(tz=tz('America/New_York')).strftime('%Y%m%d_%H%M')
    Ymd = YmdHM[:8]

    sql_report = """\
with cte0 as (
select max(cust_id) cust_id, max(pat_acct) pat_acct, pm_sk, max(created_at)::date bill_date from tpl_billing_records group by pm_sk
)
-->
select 
	 effective_date
	 , pm_sk
	 , src_sk
	 , cust_id
	 , pat_acct
	 , claim_num
	 , charges
	 , policy_number
	 , claim_type
	 , insurance_name
	 , patient_name
	 , vx_carrier_lob
	 , work_comp_flag
	 , jopari_response
	 , code
	 , processed
	 , not_done
	 , processed_at
	 , assigned
	 , bill_date
	 , bill_sent_after
	 , aging
from (
	select
		a.created_at::date
		, a.cust_id
		, concat('"',a.pat_acct,'"') pat_acct
		, concat('"',a.claim_num,'"') claim_num
		, a.pm_sk
		, a.src_sk
		, concat('$', round(a.charges)) charges
		, a.claim_type
		, a.patient_name
		, concat('"',a.policy_number,'"') policy_number
		, a.insurance_name
		, a.vx_carrier_lob
		, case when a.work_comp_flag  = 'Y' then 'T' else 'F' end work_comp_flag
		, a.note jopari_response
		, a.code 
		, a.effective_date
		, case when a.processed = true then 'Y' else 'N' end processed
		, case when a.processed = false then format('%s of %s', count(*) over (partition by a.note, a.processed), count(*) over (partition by a.note)) else null end not_done
		, a.processed_at::date
		, a.assigned 
		, b.bill_date
		, case when a.effective_date > b.bill_date then 'N' else 'Y' end bill_sent_after
		, case when processed = false then now()::date - b.bill_date else null end aging
	from 
		business_analysis.jopari_claim_alerts a
	left join 
		cte0 b on a.pm_sk = b.pm_sk
	where 
		now()::date - effective_date < 365
	) foo0 
where
	assigned ~* 'MLX Yi James'
	and processed = 'N'
order by 
	effective_date desc, pm_sk, cust_id;"""

    cur = con.cursor()
    cur.execute(sql_report)

    with open(r'L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{}.csv'.format(YmdHM), 'w') as fw:
        headers = [r[0] for r in cur.description]
        # print(headers)
        print(','.join(headers), file=fw)

        for r in cur:
            row = [str(x) for x in r]
            # print(row)
            print(','.join(row), file=fw)

    cur.close()

    print('generated csv report...')


def convert_csv_xlsx(csv):
    wb = openpyxl.Workbook()
    ws = wb.active

    with open(csv, 'r') as fr:
        for line in fr:
            line = line.strip()

            tmp = line.split(',')

            if 'charges' not in line:
                tmp[6] = '${:,}'.format(int(tmp[6].replace('$', '')))

                for i in (4, 5, 7):
                    tmp[i] = tmp[i].replace('"', '')
            # print(tmp)

            ws.append(tmp)

    wb.save(csv.replace('.csv', '.xlsx'))


def format_macro():
    YmdHM = dt.now(tz=tz('America/New_York')).strftime('%Y%m%d_%H%M')
    Ymd = YmdHM[:8]

    convert_csv_xlsx(rf"L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{YmdHM}.csv")

    wb = Book(r"L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_20211022.xlsm")
    mymacro = wb.macro('header')

    wb1 = Book(rf"L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{YmdHM}.xlsx")
    mymacro()
    wb1.save(rf"L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{YmdHM}.xlsx")

    xw.apps.active.quit()

    os.remove(rf"L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{YmdHM}.csv")

    print()
    print("""converted to...""")

    op = rf'L:\Auto_Opportunity_Analysis\jopari_alerts\Weekly_Report_Jopari_Claim_Alerts_James_{YmdHM}.xlsx'
    print(op)

    return op


def main():
    os.chdir(r'L:\Auto_Opportunity_Analysis\jopari_alerts')

    YmdHM = dt.now(tz=tz('America/New_York')).strftime('%Y%m%d_%H%M')
    Ymd = YmdHM[:8]

    params = {
        'host': 'revpgdb01.revintel.net',
        'database': 'tpliq_tracker_db',
        'user': user_db,
        'password': passwd_db
    }
    conn = psycopg2.connect(**params)

    c0 = 0

    #----
    # Run
    #----
    with conn:
        update_jopari_responses(f'alerts_{Ymd}.txt', c0, conn)    # SPECIFY
        backup_jopari_responses(conn)
        generate_james_report(conn); os.system(f'start excel.exe {format_macro()}')

    #----

        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()

