SELECT COUNT_IF(country LIKE 'U%')
FROM world_population;

SELECT * 
FROM world_population
WHERE (country LIKE 'U%');

SELECT CAST('2025-10-18' AS timestamp);

SELECT CAST(current_timestamp() AS varchar(100));

SELECT EXTRACT(YEAR FROM current_timestamp());

SELECT STRING(current_date());
