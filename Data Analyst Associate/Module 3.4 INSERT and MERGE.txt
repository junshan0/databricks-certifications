-- Create base table, with generated column
CREATE OR REPLACE TABLE events_for_insert (
    eventId BIGINT,
    eventName STRING,
    eventType STRING,
    eventTime TIMESTAMP,
    eventDate date GENERATED ALWAYS AS (CAST(eventTime AS DATE))
);

-- INSERT with INTO clause and value list
INSERT INTO events_for_insert (eventId, eventName, eventType, eventTime)
VALUES (1, 'Tom birthday', 'Party', '2024-09-01 12:00:00');

select * from events_for_insert;

-- INSERT OVERWRITE will remove all existing data
-- Compare this result against the result in previous cell which used INSERT INTO
INSERT OVERWRITE events_for_insert (eventId, eventName, eventType, eventTime)
VALUES (3, 'Jim baseball', 'Baseball', '2024-09-21 12:00:00');

SELECT * FROM events_for_insert;

-- INSERT INTO ... REPLACE will replace the existing data that matches the WHERE condition
-- Other records that do not meet the WHERE condition will be untouched
INSERT INTO events_for_insert (eventId, eventName, eventType, eventTime)
VALUES (1, 'Tom birthday', 'Party', '2024-09-01 12:00:00');

INSERT INTO events_for_insert
REPLACE WHERE eventId = 3
VALUES (3, 'Mary piano', 'Piano', '2024-09-30 12:00:00', '2024-09-30');

SELECT * FROM events_for_insert;



-- Create base table, with generated column
CREATE OR REPLACE TABLE events_for_merge (
    eventId BIGINT,
    eventName STRING,
    eventType STRING,
    eventTime TIMESTAMP,
    eventDate date GENERATED ALWAYS AS (CAST(eventTime AS DATE))
);

INSERT INTO events_for_merge (eventId, eventName, eventType, eventTime)
VALUES (2, 'Allen soccer', 'Soccer', '2024-09-11 12:00:00');
INSERT INTO events_for_merge (eventId, eventName, eventType, eventTime)
VALUES (3, 'Jim baseball', 'Baseball', '2024-09-21 12:00:00');

MERGE INTO events_for_insert USING events_for_merge
  ON events_for_insert.eventId = events_for_merge.eventId
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM events_for_insert;
