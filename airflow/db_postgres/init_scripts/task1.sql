CREATE TABLE IF NOT EXISTS historical_rates (
    id serial4 PRIMARY KEY,
    "date" DATE NOT NULL,
    first_currency VARCHAR(3) NOT NULL,
    second_currency VARCHAR(3) NOT NULL,
    rate REAL NOT NULL
)