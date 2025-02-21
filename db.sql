
CREATE TYPE client_status AS ENUM ('active', 'inactive');
CREATE TYPE dibs_type AS ENUM ('slack', 'discord');
CREATE TYPE plan_type AS ENUM ('free', 'extra', 'pro');
CREATE TYPE payment_type AS ENUM ('bmac', 'paypal', 'stripe');
CREATE TYPE payment_frequency AS ENUM ('monthly', 'yearly');
CREATE TYPE event_type AS ENUM (
    'create_free', 'create_extra', 'create_pro',
    'cancel_free', 'cancel_extra', 'cancel_pro',
    'receive_email', 'sent_email'
);

CREATE TABLE client (
    id SERIAL PRIMARY KEY,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name VARCHAR(255) NOT NULL,
    nick VARCHAR(255) NOT NULL,
    type dibs_type NOT NULL default 'slack',
    plan plan_type NOT NULL default 'free',
    status client_status NOT NULL DEFAULT 'active',
    notes TEXT,
    url VARCHAR(255),
    team VARCHAR(255) DEFAULT ''
);

CREATE TABLE contact (
    id SERIAL PRIMARY KEY,
    client_id INT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    role VARCHAR(255) default '',
    UNIQUE (email), -- Ensures contact email uniqueness
    CONSTRAINT fk_client_contact FOREIGN KEY (client_id)
        REFERENCES client(id) ON DELETE CASCADE
);

CREATE TABLE payment (
    id SERIAL PRIMARY KEY,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    client_id INT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
    type payment_type NOT NULL,
    amount NUMERIC(10, 2) NOT NULL CHECK (amount >= 0), -- Positive amount
    frequency payment_frequency NOT NULL,
    plan VARCHAR(50) NOT NULL,
    CONSTRAINT fk_client_payment FOREIGN KEY (client_id)
        REFERENCES client(id) ON DELETE CASCADE
);

CREATE TABLE event (
    id SERIAL PRIMARY KEY,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    type event_type NOT NULL,
    body JSONB NOT NULL, -- Storing additional event data
    client_id INT REFERENCES client(id) ON DELETE SET NULL, -- Event may relate to a client
    contact_id INT REFERENCES contact(id) ON DELETE SET NULL -- Event may relate to a contact
);



