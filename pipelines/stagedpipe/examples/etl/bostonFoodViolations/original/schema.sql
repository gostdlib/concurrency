CREATE TABLE IF NOT EXISTS violations (
    business_name TEXT,
    license_status TEXT,
    result TEXT,
    description TEXT,
    time TIMESTAMP,
    status TEXT,
    level INTEGER,
    comments TEXT,
    address TEXT,
    city TEXT,
    zip TEXT
);
