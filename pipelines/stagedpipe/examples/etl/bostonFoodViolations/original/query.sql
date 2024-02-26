SELECT 
    business_name, COUNT(business_name) as num_violations
FROM
    violations
WHERE
    license_status = 'Active' AND 
    time >= '2016-01-01'
GROUP BY business_name
ORDER BY num_violations DESC
LIMIT 20
