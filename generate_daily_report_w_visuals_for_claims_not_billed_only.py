import psycopg2.extras
import xlsxwriter
import time

from config import user_db, passwd_db


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]
datestr = timestr[:8]

print('start generating report... ')
print('\nL:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_Not_Billed\\'
      'MLX_Daily_Report_Opportunity_Value_Not_Billed_James_' + minutestr + '.xlsx...')


# Pass parameters to log in
parms = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': 'james_niu_u',
    'password': 'Cw18745.'
}
conn = psycopg2.connect(**parms)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

print('\nplease wait... querying...')
print('window will close automatically if failure...')
time_0 = time.time()


# Place SQL query here
sql_1 = """
/*MLX Status Summary by Insurance for Not Billed*/
select
	foo_2.mlx_pat_acct_status,
	foo_2.not_billed_record,
	foo_2.distinct_insurance_name,
	foo_3.description,
	foo_3.x12_code,
	foo_3.note
from
	(
	select 
		mlx_pat_acct_status,
		count(*) as not_billed_record, 
		count(distinct insurance_name) as distinct_insurance_name 
	from (
		select
			tpl_pre_billing_records.insurance_name,
			case 
				when tpl_client_account_statuses.status is null then tpl_client_patient_accounts.mlx_status_code
				else case
					when tpl_client_patient_accounts.mlx_status_code is null then tpl_client_account_statuses.status
					else tpl_client_patient_accounts.mlx_status_code end
				end as mlx_pat_acct_status
		from
			tpl_pre_billing_records
		left join (
			select pat_acct, string_agg(distinct status, ';  ') as status from tpl_client_account_statuses group by pat_acct) as tpl_client_account_statuses on
			tpl_pre_billing_records.pat_acct = tpl_client_account_statuses.pat_acct
		left join
			tpl_client_patient_accounts on  
			tpl_pre_billing_records.pat_acct = tpl_client_patient_accounts.pat_acct
		where
			tpl_pre_billing_records.status !='B') as foo_1
	group by 
		mlx_pat_acct_status) as foo_2
left join (
	select
		column1 as mlx_pat_acct_status,
		column2 as description,
		column3 as x12_code,
		column4 as note
	from
		(
		values 
			('MLXREQ00' ,'Medlytix request for account placement', ' ','Medlytix request for placement'), 
			('MLXACK00' ,'Medlytix acknowledges receipt of account, placement successful', ' ','MLX reserved, placement acknowledged'), 
			('MLXDUP00' ,'Medlytix returns placement as duplicate, the original account placement will remain with Medlytix', ' ','MLX reserved, placement duplicate'),
			('MLXPND00' ,'Placement accepted but pended for data edit review', ' ','MLX reserved, pended for review'),
			('MLXREJ00' ,'Placement returned for rejection (does not meet processing criteria or failed data edit review)', ' ','MLX returned, placement rejected'),
			('MLXDRP00' ,'Bill has been dropped to the carrier, may provide a carrier name', ' ','MLX reserved, dropped bill'),
			('MLXRLS01' ,'Medlytix workflows have ended and an account has been returned to the client', ' ','MLX returned, not eligible'), 
			('MLXRLS02' ,'Medlytix reserves the account for MVA payment, the client may bill the next responsible party. 
			This code may be used to trigger the client’s billing system to bill the next responsible party after a mutually defined number of days following 
			initial placement with Medlytix. Presence of “carrier” is advising that the health payer can be considered as secondary to the MVA carrier (per client’s discretion).', 
			 ' ','MLX reserved, move to next carrier'),
			('MLXRLS03' ,'Medlytix reserves the account for continued pursuit of recovery with an attorney, client should not bill other parties, 
			Medlytix may provide law firm as carrier', ' ','MLX reserved, attorney on record'),
			('MLXRLS04' ,'Agency placement returned without payment', ' ','MLX returned, without payment'),
			('MLXRLS05' ,'Agency placement returned with payment', ' ','MLX returned, with payment'),
			('MLXRLS06' ,'Medlytix reserves account with partial payment', ' ','MLX reserved, partial payment received'),
			('MLXRLS99' ,'Audit', ' ','MLX reserved, audit')
			) as foo_1 
	union
	select
		mlx_status_code as mlx_pat_acct_status,
		mlx_status_code_desc as description,
		mlx_status_code_default_x12_code as x12_code ,
		mlx_status_code_usage as note
	from 
		tpl_status_codes
	order by 
		mlx_pat_acct_status) as foo_3 on
		foo_2.mlx_pat_acct_status = foo_3.mlx_pat_acct_status
order by
	not_billed_record desc;
"""
cur.execute(sql_1)


# Prepare data in duple/list readable by xlsxwriter
data_1 = []
for list_ in cur:
    data_1.append(list_)
data_1 = tuple(data_1)
# print(data_1)

time_1 = time.time()
print('\nquery 1/6 success...' + str(round((time_1 - time_0), 1)) + 's...')


# Place SQL query here
sql_2 = """
/*Status Summary by Insurance Name for Not Billed*/
select 
	status as pre_billing_record_status,
	count(*) as not_billed_record,
	count(distinct insurance_name) as distinct_insurance_name,
	case 
		when status = 'E' then 'Exception: Data missing or error from customer; Medlytix cannot fill in crucial information such as procedure or diagnosis codes etc.'
		else case
			when status = 'PB' then 'Pre-Bill: Ready to bill'
			else case
				when status = 'PC' then 'Pre-Closed: Do not need to send; Customer already paid'
				else case
					when status = 'W' then 'Waiting: Data missing or error from non-customer sources; Medlytix can attempt to fill in such a fax #, patient zip code etc.'
						else case
							when status = 'X' then 'Withdrawn by Customer: Do not need to send; Customer may have already been paid 70%'
							else null end
						end
				end
			end
		end as description
from
	tpl_pre_billing_records
where
	status != 'B'
group by
	status
order by not_billed_record desc;
"""
cur.execute(sql_2)


# Prepare data in duple/list readable by xlsxwriter
data_2 = []
for list_ in cur:
    data_2.append(list_)
data_2 = tuple(data_2)
# print(data_2)

time_2 = time.time()
print('\nquery 2/6 success...' + str(round((time_2 - time_1), 1)) + 's...')


# Place SQL query here
sql_3 = """
/*Opportunity Analysis on Insurance Level for Not Billed*/
--select count(*) from (
select
	foo_1.insurance_name,
	foo_5.mlx_pat_acct_status,
	foo_2.pre_billing_record_status,
	foo_1.pre_billing_record,
	foo_1.not_billed_record,
	foo_2.pat_acct,
	foo_2.cust_id,
	foo_2.assigned_record_at,
	foo_1.pct_not_billed_record,
	foo_2.not_billed_avg_record_charge,
	foo_4.not_billed_tot_acct_payment,
	foo_4.not_billed_tot_acct_payer
from
	(
	select
		insurance_name,
		count(*) as pre_billing_record,
		sum(case when status != 'B' then 1 else 0 end) as not_billed_record,
		case
			when sum(case when status != 'B' then 1 else 0 end) = 0 then null
			else round(sum(case when status != 'B' then 1 else 0 end)::numeric / count(*), 2) end as pct_not_billed_record
		from
			tpl_pre_billing_records
		group by
			insurance_name) as foo_1
right join (
	select
		insurance_name,
		string_agg(distinct status, '; ') as pre_billing_record_status,
		string_agg(distinct cust_id::text, '; ') as cust_id,
		string_agg(distinct pat_acct, ';  ') as pat_acct,
		string_agg(distinct (created_at::date)::text, ';  ') as assigned_record_at,
		round(avg(charges), 0) as not_billed_avg_record_charge
	from
		tpl_pre_billing_records
	where
		status != 'B'
	group by
		insurance_name) as foo_2 on
	foo_1.insurance_name = foo_2.insurance_name
left join (
	select
		tpl_pre_billing_records.insurance_name,
		sum(foo_3.acct_payment_not_billed) as not_billed_tot_acct_payment,
		string_agg(foo_3.acct_payer_name, ';   ') as not_billed_tot_acct_payer
	from
		tpl_pre_billing_records
	left join (
		select
			pat_acct,
			round(sum(payment), 0) as acct_payment_not_billed,
			string_agg(distinct insurance_name, ';   ') as acct_payer_name
		from
			tpl_apollo_payments
		where
			duplicate_payment = false
			and not_billed = true
		group by
			pat_acct
		union
		select
			pat_acct,
			round(sum(payment), 0) as acct_payment_not_billed,
			string_agg(distinct insurance_name, ';   ') as acct_payer_name
		from
			tpl_athena_payments
		where
			duplicate_payment = false
			and not_billed = true
		group by
			pat_acct) as foo_3
		on tpl_pre_billing_records.pat_acct = foo_3.pat_acct
	where
		status != 'B'
	group by 
		insurance_name) as foo_4 on
	foo_2.insurance_name = foo_4.insurance_name
left join (
	select
		tpl_pre_billing_records.insurance_name,
		string_agg(distinct
			case 
				when tpl_client_account_statuses.status is null then tpl_client_patient_accounts.mlx_status_code
				else case
					when tpl_client_patient_accounts.mlx_status_code is null then tpl_client_account_statuses.status
					else tpl_client_patient_accounts.mlx_status_code end
				end, ';  ') as mlx_pat_acct_status
	from
		tpl_pre_billing_records
	left join (
		select pat_acct, string_agg(distinct status, ';  ') as status from tpl_client_account_statuses group by pat_acct) as tpl_client_account_statuses on
		tpl_pre_billing_records.pat_acct = tpl_client_account_statuses.pat_acct
	left join
		tpl_client_patient_accounts on  
		tpl_pre_billing_records.pat_acct = tpl_client_patient_accounts.pat_acct
	where 
		tpl_pre_billing_records.status != 'B'
	group by
		tpl_pre_billing_records.insurance_name) as foo_5
	on foo_2.insurance_name = foo_5.insurance_name
order by
	pct_not_billed_record desc,
	not_billed_avg_record_charge desc;
"""
cur.execute(sql_3)


# Prepare data in duple/list readable by xlsxwriter
data_3 = []
for list_ in cur:
    data_3.append(list_)
data_3 = tuple(data_3)
# print(data_3)

time_3 = time.time()
print('\nquery 3/6 success...' + str(round((time_3 - time_2), 1)) + 's...')


# Place SQL query here
sql_4 = """
/*MLX Status Summary by Account for Not Billed*/
select
	foo_2.mlx_pat_acct_status,
	foo_2.not_billed_record,
	foo_2.distinct_pat_acct,
	foo_3.description,
	foo_3.x12_code,
	foo_3.note
from
	(select 
		mlx_pat_acct_status,
		count(*) as not_billed_record,
		count(distinct pat_acct) as distinct_pat_acct
	from
		(select
			tpl_pre_billing_records.pat_acct,
			case 
				when tpl_client_account_statuses.status is null then tpl_client_patient_accounts.mlx_status_code
				else case
					when tpl_client_patient_accounts.mlx_status_code is null then tpl_client_account_statuses.status
					else tpl_client_patient_accounts.mlx_status_code end
				end as mlx_pat_acct_status
		from
			tpl_pre_billing_records
		left join (
			select pat_acct, string_agg(distinct status, ';  ') as status from tpl_client_account_statuses group by pat_acct) as tpl_client_account_statuses on
			tpl_pre_billing_records.pat_acct = tpl_client_account_statuses.pat_acct
		left join
			tpl_client_patient_accounts on  
			tpl_pre_billing_records.pat_acct = tpl_client_patient_accounts.pat_acct
		where
			tpl_pre_billing_records.status !='B') as foo_1
	group by 
		mlx_pat_acct_status
	order by 
		mlx_pat_acct_status) as foo_2
left join(
	select
		column1 as mlx_pat_acct_status,
		column2 as description,
		column3 as x12_code,
		column4 as note
	from
		(
		values 
			('MLXREQ00' ,'Medlytix request for account placement', ' ','Medlytix request for placement'), 
			('MLXACK00' ,'Medlytix acknowledges receipt of account, placement successful', ' ','MLX reserved, placement acknowledged'), 
			('MLXDUP00' ,'Medlytix returns placement as duplicate, the original account placement will remain with Medlytix', ' ','MLX reserved, placement duplicate'),
			('MLXPND00' ,'Placement accepted but pended for data edit review', ' ','MLX reserved, pended for review'),
			('MLXREJ00' ,'Placement returned for rejection (does not meet processing criteria or failed data edit review)', ' ','MLX returned, placement rejected'),
			('MLXDRP00' ,'Bill has been dropped to the carrier, may provide a carrier name', ' ','MLX reserved, dropped bill'),
			('MLXRLS01' ,'Medlytix workflows have ended and an account has been returned to the client', ' ','MLX returned, not eligible'), 
			('MLXRLS02' ,'Medlytix reserves the account for MVA payment, the client may bill the next responsible party. 
			This code may be used to trigger the client’s billing system to bill the next responsible party after a mutually defined number of days following 
			initial placement with Medlytix. Presence of “carrier” is advising that the health payer can be considered as secondary to the MVA carrier (per client’s discretion).', 
			 ' ','MLX reserved, move to next carrier'),
			('MLXRLS03' ,'Medlytix reserves the account for continued pursuit of recovery with an attorney, client should not bill other parties, 
			Medlytix may provide law firm as carrier', ' ','MLX reserved, attorney on record'),
			('MLXRLS04' ,'Agency placement returned without payment', ' ','MLX returned, without payment'),
			('MLXRLS05' ,'Agency placement returned with payment', ' ','MLX returned, with payment'),
			('MLXRLS06' ,'Medlytix reserves account with partial payment', ' ','MLX reserved, partial payment received'),
			('MLXRLS99' ,'Audit', ' ','MLX reserved, audit')
			) as foo_1 
	union
	select
		mlx_status_code as mlx_pat_acct_status, 
		mlx_status_code_desc as description,
		mlx_status_code_default_x12_code as x12_code ,
		mlx_status_code_usage as note
	from 
		tpl_status_codes
	order by 
		mlx_pat_acct_status) as foo_3 on
		foo_2.mlx_pat_acct_status = foo_3.mlx_pat_acct_status
order by 
	not_billed_record desc;
"""
cur.execute(sql_4)


# Prepare data in duple/list readable by xlsxwriter
data_4 = []
for list_ in cur:
    data_4.append(list_)
data_4 = tuple(data_4)
# print(data_4)

time_4 = time.time()
print('\nquery 4/6 success...' + str(round((time_4 - time_3), 1)) + 's...')


# Place SQL query here
sql_5 = """
/*Pre-Billing Status Summary by Account for Not Billed*/
select 
	status as pre_billing_record_status,
	count(*) as not_billed_record,
	count(distinct pat_acct) as distinct_pat_acct,
	case 
		when status = 'E' then 'Exception: Data missing or error from customer; Medlytix cannot fill in crucial information such as procedure or diagnosis codes etc.'
		else case
			when status = 'PB' then 'Pre-Bill: Ready to bill'
			else case
				when status = 'PC' then 'Pre-Closed: Do not need to send; Customer already paid'
				else case
					when status = 'W' then 'Waiting: Data missing or error from non-customer sources; Medlytix can attempt to fill in such a fax #, patient zip code etc.'
						else case
							when status = 'X' then 'Withdrawn by Customer: Do not need to send; Customer may have already been paid 70%'
							else null end
						end
				end
			end
		end as description
from
	tpl_pre_billing_records
where
	status != 'B'
group by
	status
order by 
	not_billed_record desc;
"""
cur.execute(sql_5)


# Prepare data in duple/list readable by xlsxwriter
data_5 = []
for list_ in cur:
    data_5.append(list_)
data_5 = tuple(data_5)
# print(data_5)

time_5 = time.time()
print('\nquery_5/6 success...' + str(round((time_5 - time_4), 1)) + 's...')


# Place SQL query here
sql_6 = """
/*Opportunity Analysis on Account Level by Not Billed*/
--select count(*) from (
select
	tpl_cust_infos.master_id,
	foo_1.cust_id,
	foo_1.pat_acct,
	foo_7.mlx_pat_acct_status,
	foo_2.pre_billing_record_status,
	foo_1.pre_billing_record,
	foo_1.billed_record,
	foo_1.not_billed_record,
	foo_1.pct_not_billed_record,
	foo_3.placed_days,
	foo_2.not_billed_acct_charge,
	foo_2.not_billed_insurance,
	foo_4.not_billed_acct_payment,
	foo_4.not_billed_acct_payer,
	foo_5.payment_date,
	foo_5.posting_date,
	foo_6.first_billing_date,
	case
		when foo_5.payment_date is not null and foo_6.first_billing_date is not null then
		case
			when foo_5.payment_date < foo_6.first_billing_date then 'Y'
			else 'N' end
		else null end as paid_bef_billing,
	case
		when foo_5.posting_date is not null and foo_6.first_billing_date is not null then
		case
			when foo_5.posting_date < foo_6.first_billing_date then 'Y'
			else 'N' end
		else null end as posted_bef_billing,
	foo_2.insured_state,
	foo_2.accident_state,
	case
		when foo_2.accident_state is not null and foo_2.insured_state is not null then
		case
			when foo_2.accident_state = foo_2.insured_state then 'Y'
			else 'N' end
		else null end as insured_eq_accident_state
from
	tpl_cust_infos
right join (
	select
		min(cust_id) as cust_id,
		pat_acct,
		count(*) as pre_billing_record,
		sum
		(case
			when status = 'B' then 1
			else 0 end) as billed_record,
		sum
		(case
			when status != 'B' then 1
			else 0 end) as not_billed_record,
		round(cast(sum(case when status != 'B' then 1 else 0 end) as numeric) / count(*), 2) as pct_not_billed_record
	from
		tpl_pre_billing_records
	group by
		pat_acct) as foo_1
	on tpl_cust_infos.cust_id = foo_1.cust_id
right join (
	select
		min(cust_id) as cust_id,
		pat_acct,
		string_agg(distinct status, '; ') as pre_billing_record_status,
		round(avg(charges), 0) as not_billed_acct_charge,
		string_agg(distinct insurance_name, '; ') as not_billed_insurance,
		string_agg(distinct content->>'cms_10_place', '') as accident_state,
		string_agg(distinct content->>'cms_7_insured_state', '') as insured_state
	from
		tpl_pre_billing_records
	where
		status != 'B'
	group by
		pat_acct) as foo_2 on
	foo_1.pat_acct = foo_2.pat_acct
left join (
	select
		pat_acct,
		timezone('UTC', now())::date - min(created_at)::date as placed_days
	from
		tpl_client_raw_bills
	group by
		pat_acct) as foo_3 on
	foo_2.pat_acct = foo_3.pat_acct
left join (
	select
		pat_acct,
		round(sum(payment), 0) as not_billed_acct_payment,
		string_agg(distinct insurance_name, ';   ') as not_billed_acct_payer
	from
		tpl_apollo_payments
	where
		duplicate_payment = false
		and not_billed = true
	group by
		pat_acct
	union
	select
		pat_acct,
		round(sum(payment), 0) as not_billed_acct_payment,
		string_agg(distinct insurance_name, ';   ') as not_billed_acct_payer
	from
		tpl_athena_payments
	where
		duplicate_payment = false
		and not_billed = true
	group by
		pat_acct) as foo_4 on
	foo_2.pat_acct = foo_4.pat_acct
left join (
	select
		pat_acct,
		min(payment_date) as payment_date,
		min(payment_post_date) as posting_date
	from
		tpl_athena_payments
	where
		duplicate_payment = false
		and not_billed = false
	group by
		pat_acct
	union
	select
		pat_acct,
		min(payment_date) as payment_date,
		min(payment_post_date) as posting_date
	from
		tpl_apollo_payments
	where
		duplicate_payment = false
		and not_billed = false
	group by
		pat_acct) as foo_5
	on foo_2.pat_acct = foo_5.pat_acct
left join (
	select
		pat_acct,
		min(bill_sent_at) as first_billing_date
	from
		tpl_billing_records
	group by
		pat_acct) as foo_6 on
	foo_2.pat_acct = foo_6.pat_acct
left join (
	select
		tpl_pre_billing_records.pat_acct,
		string_agg(distinct
			case
				when tpl_client_account_statuses.status is null then tpl_client_patient_accounts.mlx_status_code
				else case
					when tpl_client_patient_accounts.mlx_status_code is null then tpl_client_account_statuses.status
					else tpl_client_patient_accounts.mlx_status_code end
				end, ';  ') as mlx_pat_acct_status
	from
		tpl_pre_billing_records
	left join (
		select pat_acct, string_agg(distinct status, ';  ') as status from tpl_client_account_statuses group by pat_acct) as tpl_client_account_statuses on
		tpl_pre_billing_records.pat_acct = tpl_client_account_statuses.pat_acct
	left join
		tpl_client_patient_accounts on
		tpl_pre_billing_records.pat_acct = tpl_client_patient_accounts.pat_acct
	where
		tpl_pre_billing_records.pat_acct != 'B'
	group by
		tpl_pre_billing_records.pat_acct) as foo_7 on
	foo_2.pat_acct = foo_7.pat_acct
order by
	pct_not_billed_record desc,
	not_billed_acct_charge desc;
"""
cur.execute(sql_6)


# Prepare data in duple/list readable by xlsxwriter
data_6 = []
for list_ in cur:
    data_6.append(list_)
data_6 = tuple(data_6)
# print(data_6)

time_6 = time.time()
print('\nquery_6/6 success...' + str(round((time_6 - time_5), 1)) + 's... generating report file...')


########################################################################################################################
# Create Excel workbook
workbook = xlsxwriter.Workbook('L:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_Not_Billed\\'
                               'MLX_Daily_Report_Opportunity_Value_Not_Billed_James_' + minutestr + '.xlsx',
                               {'constant_memory': True})
# constant = true flushes out previous row; must write out each row by col
# add_table() cannot be used; merge_range() and set_row() only work for current row


########################################################################################################################
# Create new worksheet
worksheet_1 = workbook.add_worksheet('MLX_Status_Summary_by_Insurance')


# Adjust the column width
worksheet_1.set_column(0, 0, 25.14)
worksheet_1.set_column(1, 1, 21.43)
worksheet_1.set_column(2, 2, 30.29)
worksheet_1.set_column(3, 3, 62.86)
worksheet_1.set_column(4, 4, 62.86)
worksheet_1.set_column(5, 5, 62.86)


# Add some formats
header_format = workbook.add_format({
    'bold': True,
    'align': 'center'})
text_format = workbook.add_format({'align': 'left'})
number_format = workbook.add_format({
    'num_format': '#,##0',
    'align': "right"})
percent_format = workbook.add_format({
    'num_format': '0%',
    'align': 'right'})
date_format = workbook.add_format({
    'num_format': 'm/d/yyyy',
    'align': 'right'})


# Write some headers
worksheet_1.write(0, 0, 'mlx_pat_acct_status', header_format)
worksheet_1.write(0, 1, 'not_billed_record', header_format)
worksheet_1.write(0, 2, 'distinct_insurance_name', header_format)
worksheet_1.write(0, 3, 'description', header_format)
worksheet_1.write(0, 4, 'x12_code', header_format)
worksheet_1.write(0, 5, 'note', header_format)


# Start from the first cell
row = 1
col = 0
# Iterate over the data and write it out row by row
for mlx_acct_status, not_billed_record, distinct_insurance_name, description, x12_code, note in data_1:
    worksheet_1.write(row, col, mlx_acct_status, text_format)
    worksheet_1.write(row, col + 1, not_billed_record, number_format)
    worksheet_1.write(row, col + 2, distinct_insurance_name, number_format)
    worksheet_1.write(row, col + 3, description, text_format)
    worksheet_1.write(row, col + 4, x12_code, text_format)
    worksheet_1.write(row, col + 5, note, text_format)
    row += 1

########################################################################################################################
# Create new worksheet
worksheet_2 = workbook.add_worksheet('Pre_Billing_Summary_by_Insuranc')


# Adjust the column width
worksheet_2.set_column(0, 0, 30.71)
worksheet_2.set_column(1, 1, 21.43)
worksheet_2.set_column(2, 2, 30.29)
worksheet_2.set_column(3, 3, 62.86)


# Write some headers
worksheet_2.write(0, 0, 'pre_billing_record_status', header_format)
worksheet_2.write(0, 1, 'not_billed_record', header_format)
worksheet_2.write(0, 2, 'distinct_insurance_name', header_format)
worksheet_2.write(0, 3, 'description', header_format)


# Start from the first cell
row = 1
col = 0
# Iterate over the data and write it out row by row
for pre_billing_record_status, not_billed_record, distinct_insurance_name, description in data_2:
    worksheet_2.write(row, col, pre_billing_record_status, text_format)
    worksheet_2.write(row, col + 1, not_billed_record, number_format)
    worksheet_2.write(row, col + 2, distinct_insurance_name, number_format)
    worksheet_2.write(row, col + 3, description, text_format)
    row += 1


########################################################################################################################
# Create new worksheet
worksheet_3 = workbook.add_worksheet('Opportunity_Analysis_on_Insuran')


# Adjust the column width
worksheet_3.set_column(0, 0, 67.86)
worksheet_3.set_column(1, 1, 48.57)
worksheet_3.set_column(2, 2, 35.57)
worksheet_3.set_column(3, 3, 28.57)
worksheet_3.set_column(4, 4, 27.57)
worksheet_3.set_column(5, 5, 27.71)
worksheet_3.set_column(6, 6, 27.71)
worksheet_3.set_column(7, 7, 41.86)
worksheet_3.set_column(8, 8, 41.86)
worksheet_3.set_column(9, 9, 41.86)
worksheet_3.set_column(10, 10, 41.86)
worksheet_3.set_column(11, 11, 41.86)


# Write some headers
worksheet_3.write(0, 0, 'insurance_name', header_format)
worksheet_3.write(0, 1, 'mlx_pat_acct_status', header_format)
worksheet_3.write(0, 2, 'pre_billing_record_status', header_format)
worksheet_3.write(0, 3, 'pre_billing_record', header_format)
worksheet_3.write(0, 4, 'not_billed_record', header_format)
worksheet_3.write(0, 5, 'pat_acct', header_format)
worksheet_3.write(0, 6, 'cust_id', header_format)
worksheet_3.write(0, 7, 'assigned_record_at', header_format)
worksheet_3.write(0, 8, 'pct_not_billed_record', header_format)
worksheet_3.write(0, 9, 'not_billed_avg_record_charge', header_format)
worksheet_3.write(0, 10, 'not_billed_tot_acct_payment', header_format)
worksheet_3.write(0, 11, 'not_billed_tot_acct_payer', header_format)


# Start from the first cell
row = 1
col = 0


# Iterate over the data and write it out row by row
for insurance_name, mlx_pat_acct_status, pre_billing_record_status, pre_billing_record, not_billed_record, pat_acct, \
        cust_id, assigned_record_at, pct_not_billed_record, not_billed_avg_record_charge, not_billed_tot_acct_payment, \
        not_billed_tot_acct_payer in data_3:
    worksheet_3.write(row, col, insurance_name, text_format)
    worksheet_3.write(row, col + 1, mlx_pat_acct_status, text_format)
    worksheet_3.write(row, col + 2, pre_billing_record_status, number_format)
    worksheet_3.write(row, col + 3, pre_billing_record, number_format)
    worksheet_3.write(row, col + 4, not_billed_record, number_format)
    worksheet_3.write(row, col + 5, pat_acct, number_format)
    worksheet_3.write(row, col + 6, cust_id, number_format)
    worksheet_3.write(row, col + 7, assigned_record_at, number_format)
    worksheet_3.write(row, col + 8, pct_not_billed_record, percent_format)
    worksheet_3.write(row, col + 9, not_billed_avg_record_charge, number_format)
    worksheet_3.write(row, col + 10, not_billed_tot_acct_payment, number_format)
    worksheet_3.write(row, col + 11, not_billed_tot_acct_payer, number_format)
    row += 1


# Freeze panels anchored top left to cell
worksheet_3.freeze_panes(1, 1)


# Add filters on headings
worksheet_3.autofilter(0, 0, 0, 11)


########################################################################################################################
# Create new worksheet
# Workbook object is then used to add new worksheet via the add_worksheet() method
worksheet_4 = workbook.add_worksheet('MLX_Status_Summary_by_Account')


# Adjust the column width
worksheet_4.set_column(0, 0, 25.14)
worksheet_4.set_column(1, 1, 21.43)
worksheet_4.set_column(2, 2, 30.29)
worksheet_4.set_column(3, 3, 62.86)
worksheet_4.set_column(4, 4, 62.86)
worksheet_4.set_column(5, 5, 62.86)


# Write some headers
worksheet_4.write(0, 0, 'mlx_pat_acct_status', header_format)
worksheet_4.write(0, 1, 'not_billed_record', header_format)
worksheet_4.write(0, 2, 'distinct_pat_acct', header_format)
worksheet_4.write(0, 3, 'description', header_format)
worksheet_4.write(0, 4, 'x12_code', header_format)
worksheet_4.write(0, 5, 'note', header_format)


# Start from the first cell
row = 1
col = 0
# Iterate over the data and write it out row by row
for mlx_acct_status, not_billed_record, distinct_pat_acct, description, x12_code, note in data_4:
    worksheet_4.write(row, col, mlx_acct_status, text_format)
    worksheet_4.write(row, col + 1, not_billed_record, number_format)
    worksheet_4.write(row, col + 2, distinct_pat_acct, number_format)
    worksheet_4.write(row, col + 3, description, text_format)
    worksheet_4.write(row, col + 4, x12_code, text_format)
    worksheet_4.write(row, col + 5, note, text_format)
    row += 1


########################################################################################################################
# Create new worksheet
worksheet_5 = workbook.add_worksheet('Pre_Billing_Summary_by_Account')


# Adjust the column width
worksheet_5.set_column(0, 0, 30.71)
worksheet_5.set_column(1, 1, 21.43)
worksheet_5.set_column(2, 2, 30.29)
worksheet_5.set_column(3, 3, 62.86)


# Write some headers
worksheet_5.write(0, 0, 'pre_billing_record_status', header_format)
worksheet_5.write(0, 1, 'not_billed_record', header_format)
worksheet_5.write(0, 2, 'distinct_pat_acct', header_format)
worksheet_5.write(0, 3, 'description', header_format)


# Start from the first cell
row = 1
col = 0
# Iterate over the data and write it out row by row
for pre_billing_record_status, not_billed_record, distinct_pat_acct, description in data_5:
    worksheet_5.write(row, col, pre_billing_record_status, text_format)
    worksheet_5.write(row, col + 1, not_billed_record, number_format)
    worksheet_5.write(row, col + 2, distinct_pat_acct, number_format)
    worksheet_5.write(row, col + 3, description, text_format)
    row += 1


########################################################################################################################
# Create new worksheet
# Workbook object is then used to add new worksheet via the add_worksheet() method
worksheet_6 = workbook.add_worksheet('Opportunity_Analysis_on_Account')


# Adjust the column width
worksheet_6.set_column(0, 0, 18.57)
worksheet_6.set_column(1, 1, 16.00)
worksheet_6.set_column(2, 2, 17.86)
worksheet_6.set_column(3, 3, 31.71)
worksheet_6.set_column(4, 4, 35.57)
worksheet_6.set_column(5, 5, 27.29)
worksheet_6.set_column(6, 6, 22.71)
worksheet_6.set_column(7, 7, 28.00)
worksheet_6.set_column(8, 9, 33.29)
worksheet_6.set_column(9, 9, 21.43)
worksheet_6.set_column(10, 10, 33.86)
worksheet_6.set_column(11, 11, 36.14)
worksheet_6.set_column(12, 12, 36.71)
worksheet_6.set_column(13, 13, 36.71)
worksheet_6.set_column(14, 14, 23.86)
worksheet_6.set_column(15, 15, 22.71)
worksheet_6.set_column(16, 16, 26.71)
worksheet_6.set_column(17, 17, 26.00)
worksheet_6.set_column(18, 18, 29.00)
worksheet_6.set_column(19, 19, 23.29)
worksheet_6.set_column(20, 20, 25.00)
worksheet_6.set_column(21, 21, 39.14)


# Write some headers
worksheet_6.write(0, 0, 'master_id', header_format)
worksheet_6.write(0, 1, 'cust_id', header_format)
worksheet_6.write(0, 2, 'pat_acct', header_format)
worksheet_6.write(0, 3, 'mlx_pat_acct_status', header_format)
worksheet_6.write(0, 4, 'pre_billing_record_status', header_format)
worksheet_6.write(0, 5, 'pre_billing_record', header_format)
worksheet_6.write(0, 6, 'billed_record', header_format)
worksheet_6.write(0, 7, 'not_billed_record', header_format)
worksheet_6.write(0, 8, 'pct_not_billed_record', header_format)
worksheet_6.write(0, 9, 'placed_days', header_format)
worksheet_6.write(0, 10, 'not_billed_acct_charge', header_format)
worksheet_6.write(0, 11, 'not_billed_insurance', header_format)
worksheet_6.write(0, 12, 'not_billed_acct_payment', header_format)
worksheet_6.write(0, 13, 'not_billed_acct_payer', header_format)
worksheet_6.write(0, 14, 'payment_date', header_format)
worksheet_6.write(0, 15, 'posting_date', header_format)
worksheet_6.write(0, 16, 'first_billing_date', header_format)
worksheet_6.write(0, 17, 'paid_bef_billing', header_format)
worksheet_6.write(0, 18, 'posted_before_billing', header_format)
worksheet_6.write(0, 19, 'insured_state', header_format)
worksheet_6.write(0, 20, 'accident_state', header_format)
worksheet_6.write(0, 21, 'insured_eq_accident_state', header_format)


# Start from the first cell
row = 1
col = 0


# Iterate over the data and write it out row by row
for master_id, cust_id, pat_acct, mlx_pat_acct_status, pre_billing_record_status, pre_billing_record, \
        billed_record, not_billed_record, pct_not_billed_record, placed_days, not_billed_acct_charge, \
        not_billed_insurance, not_billed_acct_payment, not_billed_acct_payer, payment_date, posting_date, \
        first_billing_date, paid_before_billing, posted_before_billing, insured_state, accident_state, \
        insured_eq_accident_state in data_6:
    worksheet_6.write(row, col, master_id, number_format)
    worksheet_6.write(row, col + 1, cust_id, number_format)
    worksheet_6.write(row, col + 2, pat_acct, text_format)
    worksheet_6.write(row, col + 3, mlx_pat_acct_status, text_format)
    worksheet_6.write(row, col + 4, pre_billing_record_status, text_format)
    worksheet_6.write(row, col + 5, pre_billing_record, number_format)
    worksheet_6.write(row, col + 6, billed_record, number_format)
    worksheet_6.write(row, col + 7, not_billed_record, number_format)
    worksheet_6.write(row, col + 8, pct_not_billed_record, percent_format)
    worksheet_6.write(row, col + 9, placed_days, number_format)
    worksheet_6.write(row, col + 10, not_billed_acct_charge, number_format)
    worksheet_6.write(row, col + 11, not_billed_insurance, text_format)
    worksheet_6.write(row, col + 12, not_billed_acct_payment, number_format)
    worksheet_6.write(row, col + 13, not_billed_acct_payer, text_format)
    worksheet_6.write(row, col + 14, payment_date, date_format)
    worksheet_6.write(row, col + 15, posting_date, date_format)
    worksheet_6.write(row, col + 16, first_billing_date, date_format)
    worksheet_6.write(row, col + 17, paid_before_billing, text_format)
    worksheet_6.write(row, col + 18, posted_before_billing, text_format)
    worksheet_6.write(row, col + 19, insured_state, text_format)
    worksheet_6.write(row, col + 20, accident_state, text_format)
    worksheet_6.write(row, col + 21, insured_eq_accident_state, text_format)
    row += 1


# Freeze panels anchored top left to cell
worksheet_6.freeze_panes(1, 3)


# Add filters on headings
worksheet_6.autofilter(0, 0, 0, 21)
########################################################################################################################
# Create new worksheet
worksheet_7 = workbook.add_worksheet('Aging_on_Account_Charge')


# Create a scatter chart object
chart_1 = workbook.add_chart({'type': 'scatter'})


# Configure the first series
# '=Sheet_1!$A$1' is equivalent to ['Sheet_1', 0, 0]
# Name of series goes to legend
chart_1.add_series({
    'categories': ['Opportunity_Analysis_on_Account', 1, 9, 100000, 9],
    'values': ['Opportunity_Analysis_on_Account', 1, 10, 100000, 10],
    'marker': {
        'type': 'diamond',
        'size': 9,
        'border': {'color': 'green'},
        'fill': {'color': 'black'}
    }
})


# Delete/hide series from the legend
chart_1.set_legend({'delete_series': [0, 1]})


# Format chart title and axes
chart_1.set_title({'name': 'Aging of Not Billed Account Charge'})
chart_1.set_x_axis({
    'name': 'placed_days',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 13},
    'min': 0.00,
    'max': 550
})
chart_1.set_y_axis({
    'name': 'not_billed_acct_charge',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 13},
    'min': 0,
    'max': 90000
})


# Resize chart by scaling
chart_1.set_size({
    'x_scale': 2.00,
    'y_scale': 3.7
})


# Add chart to the worksheet anchored top left to cell
worksheet_7.insert_chart(1, 1, chart_1, {'object_position': 3})


# Create a scatter chart object
chart_2 = workbook.add_chart({'type': 'scatter'})


# Configure the first series
# '=Sheet_1!$A$1' is equivalent to ['Sheet_1', 0, 0]
# Name of series goes to legend
chart_2.add_series({
    'categories': ['Opportunity_Analysis_on_Account', 1, 9, 100000, 9],
    'values': ['Opportunity_Analysis_on_Account', 1, 10, 100000, 10],
    'marker': {
        'type': 'diamond',
        'size': 9,
        'border': {'color': 'green'},
        'fill': {'color': 'black'}
    }
})


# Delete/hide series from the legend
chart_2.set_legend({'delete_series': [0, 1]})


# Format chart title and axes
chart_2.set_title({'name': 'Aging of Not Billed Account Charge (Removed Outliers)'})
chart_2.set_x_axis({
    'name': 'placed_days',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 14},
    'min': 0.00,
    'max': 200
})
chart_2.set_y_axis({
    'name': 'not_billed_acct_charge',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 14},
    'min': 0,
    'max': 10000
})


# Resize chart by scaling
chart_2.set_size({
    'x_scale': 2.00,
    'y_scale': 3.70
})


# Add chart to the worksheet anchored top left to cell
worksheet_7.insert_chart(1, 17, chart_2, {'object_position': 3})


########################################################################################################################
# Close the workbook
workbook.close()


# Print time taken to complete and prompt user for exit
time_7 = time.time()
print('\nend of file export to folder... ' + str(round((time_7 - time_6), 1)) + 's...')
print('L:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_Not_Billed')
print('\ndate_time completed: ' + minutestr + '... copy file path and press any key to end')

input()