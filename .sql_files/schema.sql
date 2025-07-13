CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    city text NOT NULL,
    location_type text NOT NULL,
    zipcode text NOT NULL,
    borough text NOT NULL
);

CREATE TABLE IF NOT EXISTS incident (
    incident_id INTEGER PRIMARY KEY, 
    agency text NOT NULL, 
    complaint_type TEXT NOT NULL,
    location_id INT NOT NULL,
    descriptor text NOT NULL,
    incident_status text NOT NULL, 
    created_date DATE NOT NULL, 
    closed_date DATE NOT NULL, 
    FOREIGN KEY (location_id) REFERENCES locations (id)
);