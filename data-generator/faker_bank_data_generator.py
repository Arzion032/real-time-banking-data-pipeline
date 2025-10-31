import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=env_path)

# -----------------------------
# Configuration
# -----------------------------
NUM_CUSTOMERS = 5
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 25
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"
SLEEP_SECONDS = 2
DEFAULT_LOOP = True

# CLI override (run once mode)
parser = argparse.ArgumentParser(description="Generate fake banking data.")
parser.add_argument("--once", action="store_true", help="Run only one iteration.")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """Return a rounded Decimal with 2 decimal places."""
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# -----------------------------
# Connect to PostgreSQL
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
print("🔌 Connected to database:")
print(f"  Host: {os.getenv('POSTGRES_HOST')}")
print(f"  Port: {os.getenv('POSTGRES_PORT')}")
print(f"  DB:   {os.getenv('POSTGRES_DB')}")
print(f"  User: {os.getenv('POSTGRES_USER')}")

conn.autocommit = True
cur = conn.cursor()

# -----------------------------
# Core Logic
# -----------------------------
def create_customers():
    """Generate and insert fake customers."""
    print("||------------------------------------------------------------------")
    print("|| Generating Fake Customer Data")
    print("||------------------------------------------------------------------")

    customers = []
    for i in range(NUM_CUSTOMERS):
        print(f"  Creating customer {i+1}/{NUM_CUSTOMERS}...")
        try:
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email()
            phone = fake.phone_number()[:20] 
            address = fake.address().replace("\n", ", ")

            cur.execute(
                """
                INSERT INTO customers (first_name, last_name, email, phone, address)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (first_name, last_name, email, phone, address),
            )
            customers.append(cur.fetchone()[0])
        except Exception as e:
            print(f"❌ Error creating customer: {e}")
            raise
    return customers

def create_accounts(customers):
    """Create multiple accounts per customer."""
    print("||------------------------------------------------------------------")
    print("|| Generating Fake Account Data")
    print("||------------------------------------------------------------------")
    accounts = []
    total_accounts = len(customers) * ACCOUNTS_PER_CUSTOMER
    account_count = 0
    
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_count += 1
            print(f"  Creating account {account_count}/{total_accounts}...")
            try:
                account_type = random.choice(["SAVINGS", "CHECKING", "CREDIT"])
                initial_balance = random_money(Decimal("50.00"), Decimal("2000.00"))
                cur.execute(
                    """
                    INSERT INTO accounts (customer_id, account_type, balance, currency)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (customer_id, account_type, initial_balance, CURRENCY),
                )
                accounts.append(cur.fetchone()[0])
            except Exception as e:
                print(f"❌ Error creating account: {e}")
                raise
    return accounts

def create_transactions(accounts):
    """Generate realistic transactions."""
    print("||------------------------------------------------------------------")
    print("|| Generating Transactions Account Data")
    print("||------------------------------------------------------------------")
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    successful = 0
    
    for i in range(NUM_TRANSACTIONS):
        print(f"  Creating transaction {i+1}/{NUM_TRANSACTIONS}...")
        try:
            account_id = random.choice(accounts)
            txn_type = random.choice(txn_types)
            amount = random_money(Decimal("5.00"), Decimal(str(MAX_TXN_AMOUNT)))
            related_account = None
            direction = None

            # Decide direction and related account
            if txn_type == "DEPOSIT":
                direction = "CREDIT"
            elif txn_type == "WITHDRAWAL":
                direction = "DEBIT"
            elif txn_type == "TRANSFER":
                direction = "DEBIT"
                # pick a different account as target
                possible_targets = [a for a in accounts if a != account_id]
                if possible_targets:
                    related_account = random.choice(possible_targets)

            description = f"{txn_type.capitalize()} of {amount} {CURRENCY}"

            cur.execute(
                """
                INSERT INTO transactions (account_id, txn_type, direction, amount, related_account_id, description, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'COMPLETED')
                """,
                (account_id, txn_type, direction, amount, related_account, description),
            )
            successful += 1
        except Exception as e:
            print(f"    ⚠️  Skipped: {str(e).split('CONTEXT')[0].strip()}")
            continue
    
    print(f" Successfully created {successful}/{NUM_TRANSACTIONS} transactions")

# -----------------------------
# Main Execution Loop
# -----------------------------
def run_iteration(iteration):
    print(f"\n--- Iteration {iteration} started ---")
    customers = create_customers()
    accounts = create_accounts(customers)
    create_transactions(accounts)
    print("||------------------------------------------------------------------")
    print(f"|| Created {len(customers)} customers, {len(accounts)} accounts, and {NUM_TRANSACTIONS} transactions.")
    print(f"||--- Iteration {iteration} finished ---")
    print("||------------------------------------------------------------------")
    print()
    print()

try:
    iteration = 0
    while True:
        iteration += 1
        run_iteration(iteration)
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)
