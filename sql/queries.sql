
CREATE SCHEMA chicago_dmv;

CREATE TABLE chicago_dmv.Vehicle
(             
    crash_unit_id INT PRIMARY KEY,
    crash_record_id TEXT,
    rd_no TEXT,
    crash_date TIMESTAMP,
    unit_no INT,
    unit_type VARCHAR(255),
    vehicle_id INT,
    make  TEXT,
    model  TEXT,
    lic_plate_state VARCHAR(255),
    vehicle_defect  VARCHAR(255),
    maneuver  VARCHAR(255),
    occupant_cnt  FLOAT   
);

CREATE TABLE chicago_dmv.Crash
(
    crash_record_id TEXT,
    crash_date TIMESTAMP,
    posted_speed_limit INT,
    traffic_control_device VARCHAR(255),
    weather_condition VARCHAR(255),
    lighting_condition VARCHAR(255),
    crash_hour INT,
    crash_day_of_week INT,
    crash_month INTEGER,
    latitude FLOAT,
    longitude FLOAT
);
ALTER TABLE chicago_dmv.Vehicle
ALTER COLUMN vehicle_id TYPE FLOAT 

ALTER TABLE chicago_dmv.Crash
ADD PRIMARY KEY (crash_record_id);

SELECT * FROM chicago_dmv.Crash
SELECT * FROM chicago_dmv.vehicle