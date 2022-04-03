SELECT 
	TOP 1
	j.datasource, 
	j.[datetime]
FROM
	INT_DATALAKE_SILVER.dbo.TBL_REFINED_DATA j
INNER JOIN
	(SELECT 
		TOP 2
		j.region as Region, 
		COUNT(j.region) as 'Appearing'
	FROM
		INT_DATALAKE_SILVER.dbo.TBL_REFINED_DATA j 
	GROUP BY
		j.region 
	ORDER BY 
		'Appearing' DESC 
	) TR on TR.Region = j.region  
GROUP BY
	j.datasource, j.datetime  
ORDER BY 
	datetime DESC 