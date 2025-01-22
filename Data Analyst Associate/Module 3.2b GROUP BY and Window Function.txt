CREATE OR REPLACE TABLE transaction (
	TransactionID VARCHAR(4),
	CustomerID VARCHAR(7),
	StoreID VARCHAR(3),
	TransactionDate DATE,
	TransactionAmount INT
);

INSERT INTO transaction VALUES ('T111','1-2-333','S1','2013-01-01',172);
INSERT INTO transaction VALUES ('T222','2-3-444','S2','2013-01-01',514);
INSERT INTO transaction VALUES ('T333','1-2-333','S3','2013-01-02',107);
INSERT INTO transaction VALUES ('T444','3-4-555','S3','2013-01-02',664);
INSERT INTO transaction VALUES ('T555','2-3-444','S3','2013-01-02',828);
INSERT INTO transaction VALUES ('T666','5-6-777','S10','2013-01-03',186);
INSERT INTO transaction VALUES ('T777','6-7-888','S13','2013-01-03',424);
INSERT INTO transaction VALUES ('T888','8-9-000','S4','2013-01-04',328);
INSERT INTO transaction VALUES ('T999','4-5-666','S6','2013-01-04',642);
INSERT INTO transaction VALUES ('T101','7-8-999','S12','2013-01-04',696);
INSERT INTO transaction VALUES ('T202','0-1-222','S8','2013-01-04',25);
INSERT INTO transaction VALUES ('T303','4-5-666','S6','2013-01-05',197);
INSERT INTO transaction VALUES ('T404','8-9-000','S6','2013-01-05',643);
INSERT INTO transaction VALUES ('T505','6-7-888','S14','2013-01-05',473);
INSERT INTO transaction VALUES ('T606','0-1-222','S11','2013-01-06',660);
INSERT INTO transaction VALUES ('T707','5-6-777','S4','2013-01-06',232);
INSERT INTO transaction VALUES ('T808','7-8-999','S9','2013-01-06',982);
INSERT INTO transaction VALUES ('T909','5-6-777','S4','2013-01-06',742);
INSERT INTO transaction VALUES ('T011','8-9-000','S7','2013-01-07',860);
INSERT INTO transaction VALUES ('T022','9-0-111','S5','2013-01-07',910);

-- This SQL statement will fail due to unaggregated non-group-by column in the SELECT clause
SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY StoreID;
/* ERROR MESSAGE: 
[MISSING_AGGREGATION] The non-aggregating expression "CustomerID" is based on columns which are not participating in the GROUP BY clause.
Add the columns or the expression to the GROUP BY, aggregate the expression, or use "any_value(CustomerID)" if you do not care which of the values within a group is returned. SQLSTATE: 42803
*/

-- This SQL statement will fail due to missing the GROUP BY clause
SELECT StoreID, SUM(TransactionAmount)
FROM transaction;
/* ERROR MESSAGE:
[MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY or turn it into the window functions using OVER clauses. SQLSTATE: 42803; line 1, pos 0
*/

SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY ALL
ORDER BY 1, 2;

SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY GROUPING SETS (
	(StoreID, CustomerID),
	StoreID, 
	CustomerID,
	()
)
ORDER BY 1, 2;

-- Cube is the same as GROUPING SET with all combinations
SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY CUBE(StoreID, CustomerID)
ORDER BY 1, 2;

SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY StoreID, CustomerID WITH CUBE
ORDER BY 1, 2;

-- Rollup is the same as GROUPING SET with left to right rollup combinations
SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY ROLLUP(StoreID, CustomerID)
ORDER BY 1, 2;
-- Same as (), (StoreID), (StoreID, CustomerID)
-- But no (CustomerID)

SELECT StoreID, CustomerID, SUM(TransactionAmount)
FROM transaction 
GROUP BY StoreID, CustomerID WITH ROLLUP
ORDER BY 1, 2;



SELECT   
    TransactionDate,
    CustomerID,
    RANK() OVER (PARTITION BY TransactionDate ORDER BY TransactionAmount DESC) AS rank
FROM transaction
ORDER BY 1, 3;

SELECT   
    TransactionDate,
    CustomerID,
    PERCENT_RANK() OVER (PARTITION BY TransactionDate ORDER BY TransactionAmount DESC) AS rank
FROM transaction
ORDER BY 1, 3;

