--.timer on
select
	o_orderpriority,
	count(*) as order_count
from
	orders indexed by O_ODOP_KEY
where
	o_orderdate >= "1993-07-01" --'[DATE]'
	and o_orderdate < "1993-10-01" --'[DATE]'
	and exists (
		select
            l_shipdate, l_commitdate, l_receiptdate
		from
			lineitem indexed by L_OCR_KEY
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
			)
group by
	o_orderpriority
order by
	o_orderpriority;
