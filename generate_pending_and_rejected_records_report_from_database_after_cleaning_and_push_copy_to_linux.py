#! /usr/bin/env python3
import time
import os
import sys

import pandas as pd
import psycopg2.extras
import paramiko

from paramiko import SSHClient
from scp import SCPClient

from config import user_db, passwd_db, user_linus, passwd_linux


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]
datestr = timestr[:8]
time_0 = time.time()

print('start generating report... ')
print('\nplease wait...')
print('window will close instantly if login failure...')

########################################################################################################################

params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)
# Place SQL query here
sql1 = """\
with cte2 as (
select
        created_at::date
        , 'tpl_rejected_raw_bills'::text tab
        , cust_id
        , concat('"', pat_acct, '"') pat_acct
        , coalesce(nullif(patient_firstname, ''), nullif(content->>'patient_firstname', '')) firstname
        , coalesce(nullif(patient_lastname, ''), nullif(content->>'patient_lastname', '')) lastname
        , case when nullif(content->>'patient_dob', '') is null then null else concat('dob ', (content->>'patient_dob')::date) end dob
        , case when nullif(content->>'patient_ssn', '') is null then null 
        		else concat('ssn ', '"', lpad(nullif(content->>'patient_ssn', ''), 9, '0'), '"') end ssn
        , case when coalesce(nullif(nullif(admit_date_or_service_date, '/  /'), ''), nullif(nullif(discharge_date, '/  /'), '')
                , nullif(content->>'admission_date', ''), nullif(content->>'discharge_date', ''), nullif(content->>'LX01_service_date', ''))::date is null then null
        	else concat('dos ', coalesce(nullif(nullif(admit_date_or_service_date, '/  /'), ''), nullif(nullif(discharge_date, '/  /'), '')
                , nullif(content->>'admission_date', ''), nullif(content->>'discharge_date', ''), nullif(content->>'LX01_service_date', ''))::date) end approx_dos
        , nullif(replace(case when notes ~* 'MLX' then content->>'reject_info' else notes end, ',', ';'), '') user_note
        , case when processed = 't' then 'processed Y' else ' processed N' end processed
        , case when processed_at is null then null else concat('processed at ', processed_at::date) end processed_at
        , orig_file_name data_file
from
        tpl_rejected_raw_bills
where
        cust_id not in ('67', '107', '171')
union all
select
        created_at::date
        , 'tpl_pending_raw_bills'::text tab
        , cust_id
        , concat('"', pat_acct, '"') pat_acct
        , coalesce(nullif(content->>'patient_firstname', ''), nullif(content->>'patient_first_name', '')) firstname
        , coalesce(nullif(content->>'patient_lastname', ''), nullif(content->>'patient_last_name', '')) lastname
        , case when nullif(content->>'patient_dob', '') is null then null else concat('dob ', (content->>'patient_dob')::date) end dob
        , case when nullif(content->>'patient_ssn', '') is null then null 
        		else concat('ssn ', '"', lpad(nullif(content->>'patient_ssn', ''), 9, '0'), '"') end ssn
        , case when coalesce(nullif(nullif(admit_date_or_service_date, '/  /'), ''), nullif(nullif(discharge_date, '/  /'), '')
                , nullif(content->>'admit_date_or_service_date', ''), nullif(content->>'discharge_date_or_end_service_date', ''), nullif(content->>'LX01_service_date', ''))::date is null then null
        	else concat('dos ', coalesce(nullif(nullif(admit_date_or_service_date, '/  /'), ''), nullif(nullif(discharge_date, '/  /'), '')
                , nullif(content->>'admit_date_or_service_date', ''), nullif(content->>'discharge_date_or_end_service_date', ''), nullif(content->>'LX01_service_date', ''))::date) end approx_dos
        , nullif(replace(case when notes ~* 'MLX' then coalesce(nullif(content->>'holding_info', ''), nullif(content->>'ml_reason', '')) else notes end, ', ', '; '), '') user_note
        , case when processed = 't' then 'processed Y' else 'processed N' end processed
        , case when processed_at is null then null else concat('processed at ', processed_at::date) end processed_at
        , orig_file_name data_file
from
        tpl_pending_raw_bills
where
        cust_id not in ('67', '107', '171')
)
-->
select
        cte2.created_at
        , cte2.tab "table"
        , cte2.cust_id
        , initcap(tpl_cust_infos.cust_name) cust_name
        , pat_acct
        , cte2.firstname pat_firstname
        , cte2.lastname pat_lastname
        , cte2.dob pat_dob
        , cte2.ssn pat_ssn
        , cte2.approx_dos
        , cte2.user_note
        , cte2.processed
        , cte2.processed_at
        , cte2.data_file
from
        cte2
left join
        tpl_cust_infos on
        cte2.cust_id = tpl_cust_infos.cust_id
where
        not exists (select 1 from tpl_client_raw_bills where cust_id not in ('67', '107', '171')
        	and tpl_client_raw_bills.cust_id = cte2.cust_id and tpl_client_raw_bills.pat_acct = cte2.pat_acct)
order by
        cte2.created_at desc
        , cte2.cust_id
        , cte2.pat_acct;
"""

########################################################################################################################

# Make CSV file
con = psycopg2.connect(**params)
cur = con.cursor()
cur.execute(sql1)

os.chdir('/home/james.niu@revintel.net/weekly_report_case_managers')

with open('Weekly_Report_Active_Demog_Pending_Rejected_' + minutestr + '.csv', 'w') as fw:
    cols_list = [x[0] for x in cur.description]
    cols = ','.join(cols_list)
    print(cols, file=fw)

    for line in cur:
        line_list = [str(i) for i in line]
        line = ','.join(line_list)
        print(line, file=fw)


# Convert line breaks unix2dos
UNIX_LINE_ENDING = b'\n'
WINDOWS_LINE_ENDING = b'\r\n'

file_path = 'Weekly_Report_Active_Demog_Pending_Rejected_' + minutestr + '.csv'

with open(file_path, 'rb') as fw:
    content = fw.read()

content = content.replace(UNIX_LINE_ENDING, WINDOWS_LINE_ENDING)

with open(file_path, 'wb') as fw:
    fw.write(content)

########################################################################################################################

time_1 = time.time()
print('\nsuccess generating report...' + str(round((time_1 - time_0), 1)) + 's...')
print('\nlcd L:\pending_rejected_case_managers')
print('cd /tmp/niu_report')
print('get '+ 'Weekly_Report_Active_Demog_Pending_Rejected_' + minutestr + '.csv')

########################################################################################################################

# Push a copy to linux /tmp/niu_report/'
try:
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(hostname='revproc01.revintel.net',
        username=user_linux,
        password=passwd_linux)

    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())
    scp.put('Weekly_Report_Active_Demog_Pending_Rejected_' + minutestr + '.csv', '/tmp/niu_report')

    print('\nscp transport success...')
except:
    print('\nconnection ERROR')


