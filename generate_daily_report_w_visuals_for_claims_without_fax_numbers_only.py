import time
import xlsxwriter
import psycopg2.extras

from config import user_db, passwd_db


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]
datestr = timestr[:8]

print('start generating report...')
print('\nL:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_No_Fax\\'
      'MLX_Daily_Report_Opportunity_Value_No_Fax_James_' + minutestr + '.xlsx...')


# Pass parameters to log in
parms = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db
}
conn = psycopg2.connect(**parms)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

print('\nplease wait... querying...')
print('window will close automatically if failure...')
time_0 = time.time()


# Place SQL query here
sql_1 = """
/*MLX Status Summary by Insurance for No Fax*/
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
			tpl_pre_billing_records.status in ('W', 'PB')
			and (content->>'send_fax_number' is null or content->>'send_fax_number' = '')
			and tpl_pre_billing_records.cust_id not in ('67', '171')) as foo_1
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
print('\nquery 1/3 success...' + str(round((time_1 - time_0), 1)) + 's...')


# Place SQL query here
sql_2 = """
/*Pre-Billing Status Summary by Insurance Name for No Fax*/
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
	tpl_pre_billing_records.status in ('W', 'PB')
	and (content->>'send_fax_number' is null or content->>'send_fax_number' = '')
	and tpl_pre_billing_records.cust_id not in ('67', '171')
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
print('\nquery 2/3 success...' + str(round((time_2 - time_1), 1)) + 's...')


# Place SQL query here
sql_3 = """
/*Opportunity Analysis on Insurance Level by No Fax*/
--select count(*) from (
select
	foo_1.insurance_name,
	foo_3.mlx_pat_acct_status,
	foo_2.pre_billing_record_status,
	foo_1.pre_billing_record,
	foo_1.not_billed_record,
	foo_1.no_fax,
	foo_2.cust_id,
	foo_2.pat_acct,
	foo_2.assigned_insurance_at,
	foo_1.pct_no_fax_record_not_billed,
	foo_2.no_fax_avg_record_charge
from
	(
	select
		insurance_name,
		sum(case when cust_id not in ('67', '171') then 1 else 0 end) as pre_billing_record,
		sum(case when status != 'B' and cust_id not in ('67', '171') then 1 else 0 end) as not_billed_record,
		sum(case when status in ('W', 'PB') and (content->>'send_fax_number' is null or content->>'send_fax_number' = '') and cust_id not in ('67', '171') then 1 else 0 end) 
		as no_fax,
		case
			when sum(case when status != 'B' and cust_id not in ('67', '171') then 1 else 0 end) = 0 then null
			else round(sum(case when status in ('W', 'PB') and (content->>'send_fax_number' is null or content->>'send_fax_number' = '') and cust_id not in ('67', '171') 
			then 1 else 0 end)::numeric / sum(case when status != 'B' and cust_id not in ('67', '171') then 1 else 0 end)::numeric, 2) end as pct_no_fax_record_not_billed
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
		string_agg(distinct (created_at::date)::text, ';  ') as assigned_insurance_at,
		round(avg(charges), 0) as no_fax_avg_record_charge
	from
		tpl_pre_billing_records
	where
		status in ('W', 'PB')
		and (content->>'send_fax_number' is null or content->>'send_fax_number' = '')
		and cust_id not in ('67', '171')
	group by
		insurance_name) as foo_2 on
	foo_1.insurance_name = foo_2.insurance_name
left join
	(	
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
		tpl_pre_billing_records.status in ('W', 'PB')
		and (content->>'send_fax_number' is null or content->>'send_fax_number' = '')
		and tpl_pre_billing_records.cust_id not in ('67', '171')
	group by
		tpl_pre_billing_records.insurance_name) as foo_3 on
	foo_2.insurance_name = foo_3.insurance_name
order by
	pct_no_fax_record_not_billed desc,
	no_fax_avg_record_charge desc;
"""

cur.execute(sql_3)


# Prepare data in duple/list readable by xlsxwriter
data_3 = []
for list_ in cur:
    data_3.append(list_)
data_3 = tuple(data_3)
# print(data_3)

time_3 = time.time()
print('\nquery 3/3 success...' + str(round((time_3 - time_2), 1)) + 's... generating report file...')


########################################################################################################################
# Create Excel workbook
workbook = xlsxwriter.Workbook('L:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_No_Fax\\'
                               'MLX_Daily_Report_Opportunity_Value_No_Fax_James_' + minutestr + '.xlsx',
                               {'constant_memory': True})


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
    'align': 'center'
})
text_format = workbook.add_format({'align': 'left'})
number_format = workbook.add_format({
    'num_format': '#,##0',
    'align': "right"
})
percent_format = workbook.add_format({
    'num_format': '0%',
    'align': 'right'
})


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
# Workbook object is then used to add new worksheet via the add_worksheet() method
worksheet_3 = workbook.add_worksheet('Opportunity_Analysis_on_Insuran')


# Adjust the column width
worksheet_3.set_column(0, 0, 67.86)
worksheet_3.set_column(1, 1, 37.14)
worksheet_3.set_column(2, 2, 37.14)
worksheet_3.set_column(3, 3, 28.57)
worksheet_3.set_column(4, 4, 28.00)
worksheet_3.set_column(5, 5, 24.29)
worksheet_3.set_column(6, 6, 27.71)
worksheet_3.set_column(7, 7, 27.57)
worksheet_3.set_column(8, 8, 41.86)
worksheet_3.set_column(9, 9, 41.86)
worksheet_3.set_column(10, 10, 40.00)
worksheet_3.set_column(11, 11, 40.00)


# Write some headers
worksheet_3.write(0, 0, 'insurance_name', header_format)
worksheet_3.write(0, 1, 'mlx_pat_acct_status', header_format)
worksheet_3.write(0, 2, 'pre_billing_record_status', header_format)
worksheet_3.write(0, 3, 'pre_billing_record', header_format)
worksheet_3.write(0, 4, 'not_billed_record', header_format)
worksheet_3.write(0, 5, 'no_fax_record', header_format)
worksheet_3.write(0, 6, 'cust_id', header_format)
worksheet_3.write(0, 7, 'pat_acct', header_format)
worksheet_3.write(0, 8, 'assigned_record_at', header_format)
worksheet_3.write(0, 9, 'pct_no_fax_record_not_billed', header_format)
worksheet_3.write(0, 10, 'no_fax_avg_record_charge', header_format)
worksheet_3.write(0, 11, 'note', header_format)


# Start from the first cell
row = 1
col = 0


# Iterate over the data and write it out row by row
for insurance_name, mlx_pat_acct_status, pre_billing_record_status, pre_billing_record, not_billed_record, \
    no_fax_record, cust_id, pat_acct, assigned_record_at, pct_no_fax_record_not_billed, \
    no_fax_avg_record_charge in data_3:
    worksheet_3.write(row, col, insurance_name, text_format)
    worksheet_3.write(row, col + 1, mlx_pat_acct_status, text_format)
    worksheet_3.write(row, col + 2, pre_billing_record_status, text_format)
    worksheet_3.write(row, col + 3, pre_billing_record, number_format)
    worksheet_3.write(row, col + 4, not_billed_record, number_format)
    worksheet_3.write(row, col + 5, no_fax_record, number_format)
    worksheet_3.write(row, col + 6, cust_id, number_format)
    worksheet_3.write(row, col + 7, pat_acct, text_format)
    worksheet_3.write(row, col + 8, assigned_record_at, text_format)
    worksheet_3.write(row, col + 9, pct_no_fax_record_not_billed, percent_format)
    worksheet_3.write(row, col + 10, no_fax_avg_record_charge, number_format)
    row += 1


# Freeze panels anchored top left to cell
worksheet_3.freeze_panes(1, 1)


# Add filters on headings
worksheet_3.autofilter(0, 0, 0, 11)


########################################################################################################################
# Create new worksheet
# Workbook object is then used to add new worksheet via the add_worksheet() method
worksheet_4 = workbook.add_worksheet('Distribution_on_Account_Charge')


# Create a scatter chart object
chart_1 = workbook.add_chart({'type': 'scatter'})


# Configure the first series
# '=Sheet_1!$A$1' is equivalent to ['Sheet_1', 0, 0]
# Name of series goes to legend
chart_1.add_series({
    'categories': ['Opportunity_Analysis_on_Insuran', 1, 9, 1000, 9],
    'values': ['Opportunity_Analysis_on_Insuran', 1, 10, 1000, 10],
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
chart_1.set_title({'name': 'Distribution of No Fax Avg Record Charge'})
chart_1.set_x_axis({
    'name': 'pct_no_fax_record_not_billed',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 13},
    'min': 0.00,
    'max': 1.00
})
chart_1.set_y_axis({
    'name': 'no_fax_avg_record_charge',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 13},
    'min': 0,
    'max': 10000
})


# Resize chart by scaling
chart_1.set_size({
    'x_scale': 2.00,
    'y_scale': 3.7
})


# Add chart to the worksheet anchored top left to cell
worksheet_4.insert_chart(1, 1, chart_1, {'object_position': 3})


# Create a scatter chart object
chart_2 = workbook.add_chart({'type': 'scatter'})


# Configure the first series
# '=Sheet_1!$A$1' is equivalent to ['Sheet_1', 0, 0]
# Name of series goes to legend
chart_2.add_series({
    'categories': ['Opportunity_Analysis_on_Insuran', 1, 9, 1000, 9],
    'values': ['Opportunity_Analysis_on_Insuran', 1, 10, 1000, 10],
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
chart_2.set_title({'name': 'Distribution of No Fax Avg Record Charge Distribution (Removed Outliers)'})
chart_2.set_x_axis({
    'name': 'pct_no_fax_record_not_billed',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 14},
    'min': 0.00,
    'max': 1.00
})
chart_2.set_y_axis({
    'name': 'no_fax_avg_record_charge',
    'name_font': {
        'name': 'Arial',
        'size': 14
    },
    'num_font': {'size': 14},
    'min': 0,
    'max': 2000
})


# Resize chart by scaling
chart_2.set_size({
    'x_scale': 2.00,
    'y_scale': 3.70
})


# Add chart to the worksheet anchored top left to cell
worksheet_4.insert_chart(1, 17, chart_2, {'object_position': 3})


########################################################################################################################
# Close the workbook
workbook.close()


# Print time taken to complete and prompt user for exit
time_4 = time.time()
print('\nend of file export to folder... ' + str(round((time_4 - time_3), 1)) + 's...')
print('L:\\auto_opportunity_analysis\\MLX_Daily_Reporting\\MLX_Daily_Report_No_Fax')
print('\ndate_time completed: ' + minutestr + '... copy file path and press any key to end')

input()