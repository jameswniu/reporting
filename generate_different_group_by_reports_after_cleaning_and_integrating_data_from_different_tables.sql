/*Overview of table*/
select response_from, max(created_at), count(*)
from tpl_claim_responses
group by response_from
order by max(created_at) desc

select * from public.tpl_claim_responses where response_from is not null and 
		(code = '277-F4' and tpl_claim_responses.note ~* 'A7:35')
		or (code = '277-F4' and tpl_claim_responses.note ~* 'A7:97')
		or (code = '277-F4' and tpl_claim_responses.note = 'missing Claim Number')
		or (code = '277-F4' and tpl_claim_responses.note = 'patient cannot be identified as our insured')
		or (code = '835-N587' and tpl_claim_responses.note = 'lifetime benefit maximum has been reached for this service')
		or (code = '835-N650' and tpl_claim_responses.note = 'incomplete/invalid documentation')
		or (code = '277-P1' and tpl_claim_responses.note = 'pending')
		or (code = '835-B13' and tpl_claim_responses.note ~* 'CAS code')
		or (code = '277-A7' and tpl_claim_responses.note ~* 'missing or wrong diagnosis code')
order by created_at desc


------
-- clean and integrate relevant database records info
------
--explain
--analyze
with cte0 as (
	select 
		cust_id
		, pat_acct 
		, min(created_at)::date info_received
	from 
		tpl_client_raw_bills
	where
		cust_id in (select cust_id from tpl_billing_records where timezone('UTC', now())::date - created_at::date < 180)
	group by
		cust_id
		, pat_acct
)
, cte1 as (
	select distinct
		cust_id
		, pat_acct
		, insurance_name
		, min(created_at)::date info_matched
	from 
		tpl_pre_billing_records
	group by
		cust_id
		, pat_acct
		, insurance_name
)
, cte2 as (
select 
	cte0.cust_id
	, cte0.pat_acct
	, cte0.info_received
	, cte1.insurance_name
	, cte1.info_matched
	, cte1.info_matched - cte0.info_received days_to_match
from 
	cte0 
left join 
	cte1 on 
	cte0.cust_id = cte1.cust_id and cte0.pat_acct = cte1.pat_acct
)
, cte3 as (
select 
	cust_id
	, pat_acct
	, info_received
	, string_agg(nullif(concat(info_matched::text, ' -- ', insurance_name), ' -- '), ';  ') info_matched_payer_name
	, count(info_matched) tot_matches
	, sum(case when days_to_match <= 3 then 1 else 0 end) currnt
	, sum(case when days_to_match > 3  and days_to_match <= 7 then 1 else 0 end) over_3
	, sum(case when days_to_match > 7  and days_to_match <= 30 then 1 else 0 end) over_7
	, sum(case when days_to_match > 30 and days_to_match <= 90 then 1 else 0 end) over_30
	, sum(case when days_to_match > 90 then 1 else 0 end) over_90
from 
	cte2
group by
	cust_id
	, pat_acct
	, info_received
)
-->
------
-- create group by reports
------
/*Aging group by over_xdays by accounts*/
select
	count(pat_acct) tot_accts
	, sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind) matched_accts
	, round((sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind)) / count(pat_acct)::numeric, 2) matched_accts_perc
	, count(pat_acct) - (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind)) unmatched_accts
	, round((count(pat_acct) - (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))) / count(pat_acct)::numeric, 2) unmatched_accts_perc
	, sum(currnt_ind) currnt_accts
	, round(sum(currnt_ind) / (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))::numeric, 2) currnt_of_matched_accts_perc
	, sum(over_3_ind) over_3_accts
	, round(sum(over_3_ind) / (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))::numeric, 2) over_3_of_matched_accts_perc
	, sum(over_7_ind) over_7_accts
	, round(sum(over_7_ind) / (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))::numeric, 2) over_7_of_matched_accts_perc
	, sum(over_30_ind) over_30_accts
	, round(sum(over_30_ind) / (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))::numeric, 2) over_30_of_matched_accts_perc
	, sum(over_90_ind) over_90_accts
	, round(sum(over_90_ind) / (sum(currnt_ind) + sum(over_3_ind) + sum(over_7_ind) + sum(over_30_ind) + sum(over_90_ind))::numeric, 2) over_90_of_matched_accts_perc
from (
	select
		cust_id
		, pat_acct
		, case when currnt > 0 then 1 else 0 end currnt_ind
		, case when over_3 > 0 then 1 else 0 end over_3_ind
		, case when over_7 > 0 then 1 else 0 end over_7_ind
		, case when over_30 > 0 then 1 else 0 end over_30_ind
		, case when over_90 > 0 then 1 else 0 end over_90_ind
	from
		cte3
) foo0
/*Aging group by over_xdays by matches*/
--select
--	sum(currnt) + sum(over_3) + sum(over_7) + sum(over_30) tot_matches
--	, sum(case when currnt > 0 then 1 else 0 end) 
--		+ sum(case when over_3 > 0 then 1 else 0 end) 
--		+ sum(case when over_7 > 0 then 1 else 0 end) 
--		+ sum(case when over_30 > 0 then 1 else 0 end)
--		+ sum(case when over_90 > 0 then 1 else 0 end) matched_accts
--	, round((sum(currnt) + sum(over_3) + sum(over_7) + sum(over_30))
--		/ (sum(case when currnt > 0 then 1 else 0 end) 
--			+ sum(case when over_3 > 0 then 1 else 0 end) 
--			+ sum(case when over_7 > 0 then 1 else 0 end) 
--			+ sum(case when over_30 > 0 then 1 else 0 end)
--			+ sum(case when over_90 > 0 then 1 else 0 end))::numeric, 2) matches_per_acct
--	, sum(currnt) currnt_matches
--	, sum(case when currnt > 0 then 1 else 0 end) currnt_accts
--	, round(sum(currnt)
--		/ sum(case when currnt > 0 then 1 else 0 end)::numeric, 2) matches_per_currnt_acct
--	, sum(over_3) over_3_matches
--	, sum(case when over_3 > 0 then 1 else 0 end) over_3_accts
--	, round(sum(over_3)
--		/ sum(case when over_3 > 0 then 1 else 0 end)::numeric, 2) matches_per_over_3_acct
--	, sum(over_7) over_7_matches
--	, sum(case when over_7 > 0 then 1 else 0 end) over_7_accts
--	, round(sum(over_7)
--		/ sum(case when over_7 > 0 then 1 else 0 end)::numeric, 2) matches_per_over_7_acct
--	, sum(over_30) over_30_matches
--	, sum(case when over_30 > 0 then 1 else 0 end) over_30_accts
--	, round(sum(over_30)
--		/ sum(case when over_30 > 0 then 1 else 0 end)::numeric, 2) matches_per_over_30_acct
--	, sum(over_90) over_90_matches
--	, sum(case when over_90 > 0 then 1 else 0 end) over_90_accts
--	, round(sum(over_90)
--		/ sum(case when over_90 > 0 then 1 else 0 end)::numeric, 2) matches_per_over_90_acct	
--from (
--	select
--		cust_id
--		, pat_acct
--		, currnt
--		, over_3
--		, over_7
--		, over_30
--		, over_90
--	from
--		cte3
--) foo0
/*Aging group by accounts by matches */
--select
--	cte3.cust_id
--	, tpl_cust_infos.cust_name
--	, cte3.info_received
--	, cte3.info_matched_payer_name
--	, cte3.tot_matches
--	, cte3.currnt
--	, cte3.over_3
--	, cte3.over_7
--	, cte3.over_30
--	, cte3.over_90
--from
--	cte3
--left join
--	tpl_cust_infos on
--	cte3.cust_id = tpl_cust_infos.cust_id
--order by
--	cte3.info_received desc
--	, cust_id

