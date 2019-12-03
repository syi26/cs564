--.timer on
SELECT
	s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM 
(
    SELECT 
        p_partkey, p_mfgr
    FROM
        part
    WHERE
        p_size = 11 AND p_type like 'MEDIUM BRUSHED COPPER'
)
LEFT OUTER JOIN
    partsupp
ON
    p_partkey = ps_partkey
LEFT OUTER JOIN
    supplier
ON 
    s_suppkey = ps_suppkey
LEFT OUTER JOIN
    nation
ON
    s_nationkey = n_nationkey
LEFT OUTER JOIN
    region
ON
    n_regionkey = r_regionkey
WHERE
    r_name = 'ASIA' AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp, supplier, nation, region
        WHERE
            p_partkey = ps_partkey 
            AND s_suppkey = ps_suppkey 
            AND s_nationkey = n_nationkey 
            AND n_regionkey = r_regionkey 
            AND r_name = 'ASIA'
    )
ORDER BY 
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey;
