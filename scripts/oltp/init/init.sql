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