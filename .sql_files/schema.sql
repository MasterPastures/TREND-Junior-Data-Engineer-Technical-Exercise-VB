CREATE TABLE IF NOT EXISTS locations (
    id text PRIMARY KEY,
    city text,
    zipcode text,
    borough text,
    CONSTRAINT location_unique UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS incident (
    incident_id text PRIMARY KEY, 
    agency text, 
    complaint_type text,
    location_type text,
    location_id text,
    descriptor text,
    incident_status text, 
    created_date DATE, 
    closed_date DATE, 
    FOREIGN KEY (location_id) REFERENCES locations (id),
    CONSTRAINT incident_unique UNIQUE (incident_id)
);