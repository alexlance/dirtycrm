#!/usr/bin/env python3
import os
import sys
import time
import datetime
import boto3
import sqlite3
import tempfile
from botocore.exceptions import ClientError


PAYMENT_SIGNUPS = '''
WITH RECURSIVE month_series(first_payment_month) AS (
  SELECT strftime('%Y-%m-01', '2021-01-01')
  UNION ALL
  SELECT strftime('%Y-%m-01', date(first_payment_month, '+1 month'))
  FROM month_series
  WHERE first_payment_month < strftime('%Y-%m-01', 'now')
),
first_payments AS (
  SELECT
    client.id AS client_id,
    MIN(date(payment.created)) AS first_payment_date
  FROM
    payment
  JOIN
    client ON payment.client_id = client.id
  GROUP BY
    client.id
),
clients_by_month AS (
  SELECT
    strftime('%Y-%m-01', first_payment_date) AS first_payment_month,
    COUNT(*) AS num_new_clients
  FROM
    first_payments
  GROUP BY
    strftime('%Y-%m-01', first_payment_date)
)
SELECT
  month_series.first_payment_month,
  COALESCE(clients_by_month.num_new_clients, 0) AS num_new_clients
FROM
  month_series
LEFT JOIN
  clients_by_month ON month_series.first_payment_month = clients_by_month.first_payment_month
ORDER BY
  month_series.first_payment_month;

'''


CLIENT_LIST = '''
WITH
ranked_contacts AS (
    SELECT
        ct.client_id,
        ct.name AS contact_name,
        ct.email AS contact_email,
        ROW_NUMBER() OVER (PARTITION BY ct.client_id ORDER BY (role = 'payer') DESC, role) AS rn
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
        date(MAX(created)) AS last_payment_date,
        -- Use a subquery to get the amount corresponding to the last payment date
        (SELECT amount FROM payment p WHERE p.client_id = pp.client_id ORDER BY p.created DESC LIMIT 1) AS last_payment_amount,
        (SELECT plan FROM payment p WHERE p.client_id = pp.client_id ORDER BY p.created DESC LIMIT 1) AS last_payment_plan,
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
    COALESCE(lp.last_payment_date, NULL) AS last_pay_d,
    COALESCE(lp.last_payment_amount, 0) AS last_pay,
    COALESCE(lp.total_num_payments, 0) AS payments,
    COALESCE(lp.total_amount_received, 0) AS total,
    lp.payment_type as type,
    lp.last_payment_plan AS plan
FROM client c
LEFT JOIN ranked_contacts rc ON rc.client_id = c.id AND rc.rn = 1
LEFT JOIN last_payment_info lp ON lp.client_id = c.id
LEFT JOIN recent_events re ON re.client_id = c.id
ORDER BY c.status, c.created
'''


def acquire_lock():
    s3 = boto3.client('s3', region_name=REGION)
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=LOCK_KEY,
            Body=b'locked',  # minimal content
            ContentType='text/plain',
            Metadata={
                'created': str(int(time.time()))
            },
            IfNoneMatch='*'
        )
        # print(f"Lock acquired {LOCK_KEY}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'PreconditionFailed':
            print("Lock already held")
            return False
        else:
            print(f"Unexpected error: {e}")
            raise


def release_lock():
    s3 = boto3.client('s3', region_name=REGION)
    s3.delete_object(Bucket=BUCKET, Key=LOCK_KEY)
    # print(f"Lock released {LOCK_KEY}")


def is_lock_stale():
    s3 = boto3.client('s3', region_name=REGION)
    try:
        response = s3.head_object(Bucket=BUCKET, Key=LOCK_KEY)
        created = int(response['Metadata'].get('created', '0'))
        age = int(time.time()) - created
        return age > LOCK_TTL
    except ClientError:
        return False


def table(data):
    # convert dict into list of dicts (for single row query)
    if isinstance(data, dict):
        data = [data]
    if len(data):
        headers = data[0].keys()
        col_widths = {header: max(len(header), max(len(str(row[header])) for row in data)) for header in headers}
        header_row = "  ".join(header.ljust(col_widths[header]) for header in headers)
        print(header_row)
        # Print a separator line
        print("  ".join("-" * col_widths[header] for header in headers))
        for row in data:
            print("  ".join(str(row[header]).ljust(col_widths[header]) for header in headers))
        print("")


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


def initialize_db_connection(path):
    db_path = os.environ.get('DIRTY_DB_PATH', path)  # Path to your SQLite file
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except Exception as e:
        print(f"Error initializing SQLite database connection: {e}")
        return None


def query(db, query, args):
    cursor = db.cursor()
    query = query.replace("%s", "?")  # postgres format vs sqlite format
    cursor.execute(query, args)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def insert(db, table, data):
    cursor = db.cursor()
    columns = list(data.keys())
    values = [
        value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime.datetime) else value
        for value in data.values()
    ]

    # Build query string with ? placeholders
    col_str = ', '.join(columns)
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

    try:
        cursor.execute(query, values)
        db.commit()
        return cursor.lastrowid  # ID of the inserted row
    except Exception as e:
        db.rollback()
        print(f"Error inserting record: {e}")
    finally:
        cursor.close()
    return False


def update(db, table, data, where):
    cursor = db.cursor()
    columns = list(data.keys())
    values = list(data.values())

    # Format datetime values to ISO format
    values = [
        value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime.datetime) else value
        for value in values
    ]

    # Build the SET clause like "col1 = ?, col2 = ?"
    set_clause = ", ".join(f"{col} = ?" for col in columns)

    # Build the WHERE clause like "id = ? AND name = ?"
    where_clause = " AND ".join(f"{k} = ?" for k in where.keys())

    # Final SQL string
    query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    try:
        # Combine values from data and where
        query_values = values + list(where.values())
        cursor.execute(query, query_values)
        db.commit()
        return cursor.rowcount > 0
    except Exception as e:
        db.rollback()
        print(f"Error updating record: {e}")
    finally:
        cursor.close()
    return False


# Find a client by nickname
def find_client(db, client):
    rows = query(db, "SELECT * FROM client WHERE nick LIKE %s OR name LIKE %s ORDER BY created", (f'%{client}%', f'%{client}%',))
    if len(rows) == 1:
        return rows[0]
    elif len(rows) > 1:
        for i, row in enumerate(rows, 1):
            print(f"{i}: {row['nick']} ({row['id']} {row['name']})")
        selected_index = int(input(f"Choose a client (1-{len(rows)}): ")) - 1
        return rows[selected_index]
    else:
        contact = find_contact(db, client)
        if contact:
            table(contact)
            rows = query(db, "SELECT * FROM client WHERE id = %s", (f'{contact["client_id"]}',))
            return rows[0]


def find_contact(db, contact):
    rows = query(db, "SELECT * FROM contact WHERE name LIKE %s OR email LIKE %s", (f'%{contact}%', f'%{contact}%',))
    if len(rows) == 1:
        return rows[0]
    elif len(rows) > 1:
        for i, row in enumerate(rows, 1):
            print(f"{i}: client_id: {row['client_id']} contact: {row['name']})")
        selected_index = int(input(f"Choose a contact (1-{len(rows)}): ")) - 1
        return rows[selected_index]
    else:
        print("No contact found")
        sys.exit(1)


def find_contacts_by_client_id(db, client_id):
    rows = query(db, "SELECT * FROM contact WHERE client_id = %s", (client_id,))
    if rows:
        return rows


def find_contact_by_email(db, email):
    rows = query(db, "SELECT * FROM contact WHERE email = %s", (email,))
    if rows:
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
    print("Inserting new client:")
    table(client)
    client_id = insert(db, 'client', client)
    if client_id:
        contact = {}
        contact['client_id'] = client_id
        contact["name"] = get_arg("CONTACT_NAME", "Enter contact full name")
        contact["email"] = get_arg("CONTACT_EMAIL", "Enter contact email address")
        contact["role"] = get_arg("CONTACT_ROLE", "Enter contact role (payer or blank)")
        print("Inserting new contact:")
        table(contact)
        insert(db, 'contact', contact)
    return client_id


def client_edit(db):
    print("Edit client")
    # Add your editing logic here
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    table(client)
    new = {}
    for k, v in client.items():
        new[k] = get_arg(f"CLIENT_{k.upper()}", f"Enter client {k}", v)

    update(db, "client", new, {'id': new['id']})

    print("Refetching item")
    client = find_client(db, client_nick)
    table(client)


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


def payment_signups(db):
    print("List new sign-ups over time")
    rows = query(db, PAYMENT_SIGNUPS, ())
    table(rows)


def payment_new(db):
    print("Add payment")
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    if client:
        table(client)
        payments = find_payments_by_client_id(db, client['id'])
        table(payments)
        if len(payments):
            last = len(payments) - 1
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
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    payments = find_payments_by_client_id(db, client['id'])
    table(client)
    print("")
    table(payments)


def contact_new(db):
    print("Add contact")
    client_nick = get_arg("CLIENT", "Enter client nickname")
    client = find_client(db, client_nick)
    if client:
        table(client)
        contact = {}
        contact['client_id'] = client['id']
        contact['name'] = get_arg("CONTACT_NAME", "Enter contact name")
        contact['email'] = get_arg("CONTACT_EMAIL", "Enter contact email")
        contact['role'] = get_arg("CONTACT_ROLE", "Enter contact role")
        print("New contact:")
        table(contact)
        insert(db, 'contact', contact)


def contact_edit(db):
    print("Edit contact")
    # Add your editing logic here
    contact_nick = get_arg("CONTACT", "Enter contact name or email")
    contact = find_contact(db, contact_nick)
    table(contact)
    new = {}
    for k, v in contact.items():
        new[k] = get_arg(f"CONTACT_{k.upper()}", f"Enter contact {k}", v)

    update(db, "contact", new, {'id': new['id']})

    print("Refetching item")
    contact = find_contact(db, contact_nick)
    table(contact)


def put_db_to_s3(local_db_file):
    if make_s3_backup():
        s3 = boto3.client("s3", region_name=REGION)
        s3.upload_file(
            Filename=local_db_file,
            Bucket=BUCKET,
            Key=DB_FILE
        )
    else:
        print("ERROR CANT MAKE BACKUP")


def make_s3_backup():
    s3 = boto3.client("s3", region_name=REGION)
    today = datetime.datetime.now()
    day_of_year = today.timetuple().tm_yday

    copy_source = {
        'Bucket': BUCKET,
        'Key': DB_FILE
    }
    s3.copy_object(
        Bucket=BUCKET,
        CopySource=copy_source,
        Key=f"{DB_FILE}.{day_of_year}"
    )
    print(f"Created backup: {DB_FILE}.{day_of_year}")
    return True


def fetch_db_from_s3(temp):
    s3 = boto3.client('s3', region_name=REGION)
    response = s3.get_object(Bucket=BUCKET, Key=DB_FILE)
    sqlite_bytes = response['Body'].read()
    temp.write(sqlite_bytes)
    temp.flush()  # Make sure all data is written


def main(db):
    if len(sys.argv) < 2:
        print("""Usage:

dirt.py <entity> <action>

eg:
    ./dirt.py client
    ./dirt.py client new
    ./dirt.py client show
    ./dirt.py client edit

    ./dirt.py payment new
    ./dirt.py payment show
    ./dirt.py payment signups

    ./dirt.py contact new
    ./dirt.py contact edit

""")
        sys.exit(1)

    entity = sys.argv[1]
    try:
        action = sys.argv[2]
    except IndexError:
        action = ''

    needs_sync = False

    if entity == 'client':
        if action == 'new':
            client_new(db)
            needs_sync = True
        elif action == 'edit':
            client_edit(db)
            needs_sync = True
        elif action == 'show':
            client_show(db)
        elif action == '':
            client_list(db)
        else:
            print(f"Unknown action for client: {action}")

    elif entity == 'payment':
        if action == 'new':
            payment_new(db)
            needs_sync = True
        elif action == 'edit':
            payment_edit(db)
            needs_sync = True
        elif action == 'show':
            payment_show(db)
        elif action == 'signups':
            payment_signups(db)
        else:
            print(f"Unknown action for payment: {action}")

    elif entity == 'contact':
        if action == 'new':
            contact_new(db)
            needs_sync = True
        elif action == 'edit':
            contact_edit(db)
            needs_sync = True
        else:
            print(f"Unknown action for contact: {action}")

    else:
        print(f"Unknown entity: {entity}")

    return needs_sync


if __name__ == "__main__":
    BUCKET = os.environ.get('DIRTY_BUCKET')
    DB_FILE = os.environ.get('DIRTY_DB_FILE')
    LOCK_KEY = os.environ.get('DIRTY_LOCK_KEY')
    LOCK_TTL = os.environ.get('DIRTY_LOCK_TTL')
    REGION = os.environ.get('DIRTY_REGION')
    if not DB_FILE or not BUCKET or not LOCK_KEY or not LOCK_TTL or not REGION:
        print("Missing configuration. Try: source .env")
        sys.exit(1)

    connection = None
    if acquire_lock():
        try:
            with tempfile.NamedTemporaryFile(suffix=".sqlite") as temp:
                fetch_db_from_s3(temp)
                connection = initialize_db_connection(temp.name)
                changed = main(connection)
                if changed:
                    connection.close()
                    put_db_to_s3(temp.name)
        finally:
            release_lock()
            if connection:
                connection.close()
    else:
        if is_lock_stale():
            print('lock is stale, releasing... try again')
            release_lock()
        else:
            print('Database in S3 is in use/locked, try again in 120 seconds')
