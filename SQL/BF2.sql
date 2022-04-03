SELECT 
	DISTINCT(j.region)
FROM 
	INT_DATALAKE_SILVER.dbo.TBL_REFINED_DATA j
WHERE 
	j.datasource LIKE 'cheap_mobile'