-- database initialize script

CREATE TABLE IF NOT EXISTS parking_zones (
    pz_id SERIAL PRIMARY KEY,
    district TEXT,
    address TEXT,
    is_paid BOOLEAN DEFAULT false,
    storeys_count INTEGER DEFAULT 1,
    max_places INTEGER
);

CREATE TYPE park_action AS ENUM ('entrance', 'exit');

CREATE TABLE IF NOT EXISTS parkings (
    id SERIAL PRIMARY KEY,
    pz_id INTEGER,
    storey INTEGER DEFAULT 1,
    action park_action,
    rate FLOAT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pz_id) REFERENCES parking_zones(pz_id)
);

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.fct_predictions (
    prediction_id SERIAL PRIMARY KEY,
    pz_id INTEGER,
    forecast_ts TIMESTAMP NOT NULL,
    predicted_occupancy_pct FLOAT,
    predicted_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE staging.fct_predictions 
ADD CONSTRAINT fk_predictions_parking_zone 
FOREIGN KEY (pz_id) REFERENCES public.parking_zones(pz_id);