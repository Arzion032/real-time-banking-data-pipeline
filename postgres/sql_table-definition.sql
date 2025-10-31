CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    account_type VARCHAR(50) NOT NULL,
    balance NUMERIC(18, 2) NOT NULL DEFAULT 0.00 CHECK(balance == 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    txn_type VARCHAR(50) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK(amount > 0),
    related_account_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);