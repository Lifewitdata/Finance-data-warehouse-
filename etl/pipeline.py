"""
Financial Transactions Data Warehouse — ETL Pipeline
======================================================
Simulates a fintech transactions data source and builds a proper STAR SCHEMA
data warehouse from it: one fact table (transactions) surrounded by
conformed dimension tables (customer, account, merchant, date).

Pipeline stages (extract -> transform -> load) are run as discrete, logged
steps — the same staged pattern that orchestration tools like Airflow
schedule as a DAG. This script does NOT use Airflow; it demonstrates the
underlying ETL/warehousing logic that such a tool would orchestrate.

All data is synthetic, generated for portfolio/demonstration purposes only.
"""

import logging
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("etl_pipeline")

np.random.seed(7)

DB_PATH = "data/finance_dw.db"
N_CUSTOMERS = 2000
N_ACCOUNTS = 2600
N_MERCHANTS = 150
N_TRANSACTIONS = 60000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)


# ----------------------------------------------------------------------------
# STAGE 1: EXTRACT — simulate pulling raw, messy source data
# ----------------------------------------------------------------------------
def extract():
    log.info("STAGE 1/3: EXTRACT — pulling raw source data")

    cities = ["New Delhi", "Mumbai", "Bengaluru", "Chennai", "Pune", "Hyderabad", "Kolkata"]
    segments = ["Retail", "Premium", "Business"]
    customers_raw = pd.DataFrame({
        "customer_id": [f"CUST{i:05d}" for i in range(1, N_CUSTOMERS + 1)],
        "city": np.random.choice(cities, N_CUSTOMERS),
        "segment": np.random.choice(segments, N_CUSTOMERS, p=[0.6, 0.25, 0.15]),
        "signup_date": [START_DATE - timedelta(days=int(d)) for d in np.random.randint(0, 1500, N_CUSTOMERS)],
    })
    # inject realistic messiness: some duplicate customer rows, some null cities
    dupes = customers_raw.sample(40, random_state=1)
    customers_raw = pd.concat([customers_raw, dupes], ignore_index=True)
    null_idx = customers_raw.sample(60, random_state=2).index
    customers_raw.loc[null_idx, "city"] = None

    account_types = ["Savings", "Current", "Credit Card"]
    accounts_raw = pd.DataFrame({
        "account_id": [f"ACC{i:06d}" for i in range(1, N_ACCOUNTS + 1)],
        "customer_id": np.random.choice(customers_raw["customer_id"].unique(), N_ACCOUNTS),
        "account_type": np.random.choice(account_types, N_ACCOUNTS, p=[0.5, 0.3, 0.2]),
        "opened_date": [START_DATE - timedelta(days=int(d)) for d in np.random.randint(0, 1200, N_ACCOUNTS)],
    })

    categories = ["Groceries", "Dining", "Travel", "Utilities", "Entertainment", "Healthcare", "Retail Shopping"]
    merchants_raw = pd.DataFrame({
        "merchant_id": [f"MER{i:04d}" for i in range(1, N_MERCHANTS + 1)],
        "merchant_name": [f"Merchant_{i}" for i in range(1, N_MERCHANTS + 1)],
        "category": np.random.choice(categories, N_MERCHANTS),
    })

    days_range = (END_DATE - START_DATE).days
    txn_raw = pd.DataFrame({
        "transaction_id": [f"TXN{i:07d}" for i in range(1, N_TRANSACTIONS + 1)],
        "account_id": np.random.choice(accounts_raw["account_id"], N_TRANSACTIONS),
        "merchant_id": np.random.choice(merchants_raw["merchant_id"], N_TRANSACTIONS),
        "txn_date": [START_DATE + timedelta(days=int(d)) for d in np.random.randint(0, days_range, N_TRANSACTIONS)],
        "amount": np.round(np.random.lognormal(mean=4.0, sigma=1.1, size=N_TRANSACTIONS), 2),
        "status": np.random.choice(["completed", "completed", "completed", "failed", "pending"], N_TRANSACTIONS),
    })
    # inject a few negative/garbage amounts to simulate real source-data issues
    bad_idx = txn_raw.sample(25, random_state=3).index
    txn_raw.loc[bad_idx, "amount"] = -txn_raw.loc[bad_idx, "amount"]

    log.info(f"Extracted: {len(customers_raw)} customer rows, {len(accounts_raw)} accounts, "
              f"{len(merchants_raw)} merchants, {len(txn_raw)} raw transactions")
    return customers_raw, accounts_raw, merchants_raw, txn_raw


# ----------------------------------------------------------------------------
# STAGE 2: TRANSFORM — clean source data, build conformed dimensions + fact
# ----------------------------------------------------------------------------
def transform(customers_raw, accounts_raw, merchants_raw, txn_raw):
    log.info("STAGE 2/3: TRANSFORM — cleaning and building star schema tables")

    # --- dim_customer: dedupe, fill nulls, add surrogate key ---
    dim_customer = customers_raw.drop_duplicates(subset="customer_id").copy()
    dim_customer["city"] = dim_customer["city"].fillna("Unknown")
    dim_customer = dim_customer.reset_index(drop=True)
    dim_customer.insert(0, "customer_sk", range(1, len(dim_customer) + 1))

    # --- dim_account: add surrogate key ---
    dim_account = accounts_raw.reset_index(drop=True).copy()
    dim_account.insert(0, "account_sk", range(1, len(dim_account) + 1))

    # --- dim_merchant: add surrogate key ---
    dim_merchant = merchants_raw.reset_index(drop=True).copy()
    dim_merchant.insert(0, "merchant_sk", range(1, len(dim_merchant) + 1))

    # --- dim_date: standard date dimension ---
    date_range = pd.date_range(START_DATE, END_DATE, freq="D")
    dim_date = pd.DataFrame({
        "date_sk": [int(d.strftime("%Y%m%d")) for d in date_range],
        "full_date": date_range,
        "year": date_range.year,
        "month": date_range.month,
        "month_name": date_range.strftime("%B"),
        "day_of_week": date_range.strftime("%A"),
        "is_weekend": date_range.dayofweek >= 5,
    })

    # --- clean fact source: drop negative/garbage amounts, drop rows with unresolvable FKs ---
    txn_clean = txn_raw[txn_raw["amount"] > 0].copy()
    txn_clean = txn_clean[txn_clean["account_id"].isin(dim_account["account_id"])]
    txn_clean = txn_clean[txn_clean["merchant_id"].isin(dim_merchant["merchant_id"])]

    # --- build fact_transactions: resolve natural keys -> surrogate keys ---
    fact = txn_clean.merge(dim_account[["account_id", "account_sk", "customer_id"]], on="account_id", how="left")
    fact = fact.merge(dim_customer[["customer_id", "customer_sk"]], on="customer_id", how="left")
    fact = fact.merge(dim_merchant[["merchant_id", "merchant_sk"]], on="merchant_id", how="left")
    fact["date_sk"] = fact["txn_date"].dt.strftime("%Y%m%d").astype(int)

    fact_transactions = fact[[
        "transaction_id", "customer_sk", "account_sk", "merchant_sk", "date_sk",
        "amount", "status"
    ]].reset_index(drop=True)
    fact_transactions.insert(0, "transaction_sk", range(1, len(fact_transactions) + 1))

    log.info(f"Transformed: dim_customer={len(dim_customer)} (deduped from {len(customers_raw)}), "
              f"dim_account={len(dim_account)}, dim_merchant={len(dim_merchant)}, "
              f"dim_date={len(dim_date)}, fact_transactions={len(fact_transactions)} "
              f"(dropped {len(txn_raw) - len(fact_transactions)} invalid/unresolvable rows)")

    return dim_customer, dim_account, dim_merchant, dim_date, fact_transactions


# ----------------------------------------------------------------------------
# STAGE 3: LOAD — write star schema into the warehouse (SQLite standing in
# for a cloud warehouse like Snowflake for this local demonstration)
# ----------------------------------------------------------------------------
def load(dim_customer, dim_account, dim_merchant, dim_date, fact_transactions):
    log.info("STAGE 3/3: LOAD — writing star schema tables to warehouse")
    conn = sqlite3.connect(DB_PATH)

    dim_customer.to_sql("dim_customer", conn, if_exists="replace", index=False)
    dim_account.drop(columns=["customer_id"]).to_sql("dim_account", conn, if_exists="replace", index=False)
    dim_merchant.to_sql("dim_merchant", conn, if_exists="replace", index=False)
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
    fact_transactions.to_sql("fact_transactions", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(date_sk)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_transactions(customer_sk)")
    conn.commit()
    conn.close()
    log.info(f"Load complete. Warehouse written to {DB_PATH}")


def run_pipeline():
    log.info("=" * 70)
    log.info("PIPELINE RUN START")
    customers_raw, accounts_raw, merchants_raw, txn_raw = extract()
    dim_customer, dim_account, dim_merchant, dim_date, fact_transactions = transform(
        customers_raw, accounts_raw, merchants_raw, txn_raw
    )
    load(dim_customer, dim_account, dim_merchant, dim_date, fact_transactions)
    log.info("PIPELINE RUN COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    run_pipeline()
