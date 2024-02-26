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
    :business_name,
    :license_status,
    :result,
    :description,
    :time,
    :status,
    :level,
    :comments,
    :address,
    :city,
    :zip
);
