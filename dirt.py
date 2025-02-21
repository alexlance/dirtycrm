#!/usr/bin/env python3
import os
import sys
import psycopg2
from psycopg2 import sql
import psycopg2.extras
from pprint import pprint
import datetime


CLIENT_LIST = '''
WITH
ranked_contacts AS (
    SELECT
        ct.client_id,
        ct.name AS contact_name,
        ct.email AS contact_email,
        ROW_NUMBER() OVER (PARTITION BY ct.client_id ORDER BY (role = ''), role) AS rn
    FROM contact ct
),
ranked_events AS (
    SELECT
        client_id,
        id AS event_id,
        type AS event_type,
        created AS event_created,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created DESC) AS event_rank
    FROM event
),
last_payment_info AS (
    SELECT
        client_id,
        MAX(created::date) AS last_payment_date,
        -- Use a subquery to get the amount corresponding to the last payment date
        (SELECT amount FROM payment p WHERE p.client_id = pp.client_id ORDER BY p.created DESC LIMIT 1) AS last_payment_amount,
        COUNT(*) AS total_num_payments,
        SUM(amount) AS total_amount_received,
        SUM(
            CASE
                WHEN frequency = 'monthly' THEN amount * 12
                WHEN frequency = 'yearly' THEN amount
                ELSE 0
            END
        ) AS total_amount_expected,
        MAX(type) as payment_type
    FROM payment pp
    GROUP BY client_id
),
recent_events AS (
    SELECT
        client_id,
        MAX(CASE WHEN event_rank = 1 THEN event_type END) AS recent_event,
        MAX(CASE WHEN event_rank = 2 THEN event_type END) AS prev_event,
        MAX(CASE WHEN event_rank = 3 THEN event_type END) AS prev_prev_event
    FROM ranked_events
    GROUP BY client_id
)
SELECT
    c.id AS id,
    c.name AS client_name,
    c.status AS status,
    rc.contact_name,
    rc.contact_email,
    COALESCE(lp.last_payment_date, NULL) AS last_pay_date,
    COALESCE(lp.last_payment_amount, 0) AS last_pay,
    COALESCE(lp.total_num_payments, 0) AS payments,
    COALESCE(lp.total_amount_received, 0) AS total,
    lp.payment_type
FROM client c
LEFT JOIN ranked_contacts rc ON rc.client_id = c.id AND rc.rn = 1
LEFT JOIN last_payment_info lp ON lp.client_id = c.id
LEFT JOIN recent_events re ON re.client_id = c.id
ORDER BY c.status, c.created
'''


def table(data):
    # convert dict into list of dicts (for single row query)
    if isinstance(data, dict):
        data = [data]
    headers = data[0].keys()
    col_widths = {header: max(len(header), max(len(str(row[header])) for row in data)) for header in headers}
    header_row = "  ".join(header.ljust(col_widths[header]) for header in headers)
    print(header_row)
    # Print a separator line
    print("  ".join("-" * col_widths[header] for header in headers))
    for row in data:
        print("  ".join(str(row[header]).ljust(col_widths[header]) for header in headers))


def get_arg(name, prompt, default=''):
    if name not in os.environ:
        if default:
            prompt += f' [{default}]'
        v = input(prompt + ': ')
        if v == '':
            v = default
        return v
    else:
        return os.environ.get(name)


def initialize_db_connection():
    db_config = {
        'dbname': os.environ.get('DIRTY_DB'),
        'user': os.environ.get('DIRTY_USER'),
        'password': os.environ.get('DIRTY_PASS'),
        'host': os.environ.get('DIRTY_HOST'),
        'port': os.environ.get('DIRTY_PORT')
    }
    try:
        return psycopg2.connect(**db_config)
    except Exception as e:
        print(f"Error initializing database connection: {e}")
        return None


def query(db, query, args):
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = sql.SQL(query)
    cursor.execute(query, args)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def insert(db, table, data):
    cursor = db.cursor()
    columns = list(data.keys())
    values = list(data.values())

    # fix dates from datetime.datetime.now() to normal iso format for db
    values = [
        value.strftime('%Y-%m-%d %H:%M:%S%z') if isinstance(value, datetime.datetime) else value
        for value in values
    ]
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
        sql.Identifier(table),  # Safely insert the table name
        sql.SQL(", ").join(map(sql.Identifier, columns)),  # Safely insert column names
        sql.SQL(", ").join(sql.Placeholder() * len(columns))  # Generate placeholders for values
    )

    try:
        cursor.execute(query, values)
        new_id = cursor.fetchone()[0]  # Fetch the generated ID
        db.commit()
        return new_id
    except Exception as e:
        db.rollback()
        print(f"Error inserting record: {e}")
    finally:
        cursor.close()
    return False


# Find a client by nickname
def find_client(db, client):
    rows = query(db, "SELECT * FROM client WHERE nick LIKE %s ORDER BY created", (f'%{client}%',))
    if len(rows) > 1:
        for i, row in enumerate(rows, 1):
            print(f"{i}: {row['nick']} ({row['id']} {row['name']})")
        selected_index = int(input(f"Choose a client (1-{len(rows)}): ")) - 1
        return rows[selected_index]
    else:
        return rows[0]


def find_contacts_by_client_id(db, client_id):
    rows = query(db, "SELECT * FROM contact WHERE client_id = %s", (client_id,))
    return rows


def find_contact_by_email(db, email):
    rows = query(db, "SELECT * FROM contact WHERE email = %s", (email,))
    return rows[0]


def find_payments_by_client_id(db, client_id):
    rows = query(db, "SELECT * FROM payment WHERE client_id = %s ORDER BY created", (client_id,))
    return rows


def client_new(db):
    print("Add client")
    client = {}
    client["name"] = get_arg("CLIENT_NAME", "Enter client full name")
    client["nick"] = get_arg("CLIENT_NICK", "Enter client Slack nickname")
    client["plan"] = get_arg("CLIENT_PLAN", "Enter client plan (extra, pro)", "extra")
    client["type"] = get_arg("CLIENT_TYPE", "Enter client type (slack, discord)", "slack")
    client["url"] = get_arg("CLIENT_URL", "Enter client's website URL")
    client["created"] = datetime.datetime.now()
    client["status"] = "active"
    client["team"] = get_arg("CLIENT_TEAM", "Enter client's Slack Team ID")
    print(f"Inserting new client:")
    table(client)
    client_id = insert(db, 'client', client)
    if client_id:
        contact = {}
        contact['client_id'] = client_id
        contact["name"] = get_arg("CONTACT_NAME", "Enter contact full name")
        contact["email"] = get_arg("CONTACT_EMAIL", "Enter contact email address")
        contact["role"] = get_arg("CONTACT_ROLE", "Enter contact role (payer or blank)")
        print(f"Inserting new contact:")
        table(contact)
        insert(db, 'contact', contact)


def client_edit(db):
    print("Edit client")
    # Add your editing logic here


def client_show(db):
    print("Show client")
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    contacts = find_contacts_by_client_id(db, client['id'])
    table(client)
    print("")
    table(contacts)


def client_list(db):
    print("List clients")
    rows = query(db, CLIENT_LIST, ())
    table(rows)


def payment_new(db):
    print("Add payment")
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    if client:
        payments = find_payments_by_client_id(db, client['id'])
        table(payments)
        if len(payments):
            last = len(payments)-1
            defaults = {}
            defaults['amount'] = payments[last]['amount']
            defaults['client_id'] = payments[last]['client_id']
            defaults['frequency'] = payments[last]['frequency']
            defaults['plan'] = payments[last]['plan']
            defaults['type'] = payments[last]['type']
            defaults['created'] = datetime.datetime.now()

            payment = {}
            for k, v in defaults.items():
                payment[k] = get_arg(k, f"Enter payment {k}", v)
            print("New payment:")
            table(payment)
            insert(db, 'payment', payment)
        else:
            defaults = {}
            defaults['client_id'] = client['id']
            defaults["amount"] = get_arg("PAYMENT_AMOUNT", "Enter payment amount")
            defaults['frequency'] = get_arg("PAYMENT_FREQ", "Enter payment frequency (monthly, yearly)")
            defaults['plan'] = get_arg("PAYMENT_PLAN", "Enter payment plan (extra_9, pro_49)")
            defaults['type'] = get_arg("PAYMENT_TYPE", "Enter payment type (stripe, paypal, bmac)")
            defaults['created'] = datetime.datetime.now()
            print("New payment:")
            table(defaults)
            insert(db, 'payment', defaults)


def payment_edit(db):
    print("Edit payment")


def payment_show(db):
    print("Show payment")


def main(db):

    if len(sys.argv) < 2:
        print("""Usage:

dirt.py <entity> <action>

eg:
    ./dirt.py client
    ./dirt.py client new
    ./dirt.py client show

    ./dirt.py payment new
""")
        sys.exit(1)

    entity = sys.argv[1]
    try:
        action = sys.argv[2]
    except IndexError:
        action = ''

    if entity == 'client':
        if action == 'new':
            client_new(db)
        elif action == 'edit':
            client_edit(db)
        elif action == 'show':
            client_show(db)
        elif action == '':
            client_list(db)
        else:
            print(f"Unknown action for client: {action}")

    elif entity == 'payment':
        if action == 'new':
            payment_new(db)
        elif action == 'edit':
            payment_edit(db)
        elif action == 'show':
            payment_show(db)
        else:
            print(f"Unknown action for payment: {action}")
    else:
        print(f"Unknown entity: {entity}")

if __name__ == "__main__":
    try:
        connection = initialize_db_connection()
        main(connection)
    finally:
        connection.close()
