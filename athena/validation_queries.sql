SELECT as_of_date, COUNT(*) 
FROM globalpartners_curated.fact_order_revenue
GROUP BY 1
ORDER BY 1 DESC;
