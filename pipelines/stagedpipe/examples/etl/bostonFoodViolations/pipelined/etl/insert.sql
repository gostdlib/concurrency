INSERT INTO violations (
    business_name,
    license_status,
    result,
    description,
    time,
    status,
    level,
    comments,
    address,
    city,
    zip
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11
);