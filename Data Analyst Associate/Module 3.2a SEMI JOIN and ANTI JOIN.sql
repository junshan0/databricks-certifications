CREATE OR REPLACE TABLE program (
	program_id int,
	program_name varchar(100)
);

INSERT INTO program VALUES (1, 'Holiday Special');
INSERT INTO program VALUES (2, 'World Cup Promotion');

CREATE OR REPLACE TABLE program_customer (
	program_id int,
	customer_id int
);

INSERT INTO program_customer VALUES (1, 101);
INSERT INTO program_customer VALUES (1, 102);

SELECT * 
FROM program p
SEMI JOIN program_customer pc
ON p.program_id = pc.program_id;

SELECT * 
FROM program p
ANTI JOIN program_customer pc
ON p.program_id = pc.program_id;
