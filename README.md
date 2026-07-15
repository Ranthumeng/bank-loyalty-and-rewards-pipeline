# Bank Loyalty & Rewards Pipeline

*Inspired by Nedbank's Greenbacks rewards initiative*

An end-to-end, event-driven data pipeline that simulates a bank card-spend feed and turns it into a loyalty points and rewards data mart, built on Databricks using the Medallion architecture (Bronze → Silver → Gold).

The pipeline generates realistic, live-paced synthetic South African banking transactions, streams them through Delta Lake, applies Greenbacks-style points-earning business rules, and produces analytics-ready gold tables for customer rewards dashboards and category-level reporting.

## Architecture

```Arcitecture diagram will be added```

- **Bronze layer** - untransformed, append-only Delta streaming table of every raw JSON transaction event exactly as it landed, with file-level lineage metadata (source file, ingestion time).
- **Silver layer** - deduplicated, schema-conformed transactions joined against the Greenbacks rules tables to calculate points earned per transaction and a validated running cumulative points balance per customer.
- **Gold layer** - a set of aggregated data mart tables built for downstream BI and dashboarding: customer rewards summaries, monthly point trends, category spend breakdowns, and a category leaderboard.

## Repository Structure

```
scripts/
├── synthetic_population.py           # Realistic, live-paced synthetic transaction stream generator
├── micro_batch_ingestion.py          # Bronze layer: PySpark Structured Streaming + Databricks Autoloader ingestion
└── architecture/
    ├── bronze/
    │   └── bronze.sql                # Bronze layer: streaming table via CREATE OR REFRESH STREAMING TABLE
    ├── silver/
    │   ├── delta_rules_table.sql     # Greenbacks points-earning rule tables (config, earn rates, category/merchant rules)
    │   └── silver.py                 # Silver layer: cleaning, rules application, points calculation, cumulative totals
    └── gold/
        └── data_mart.py              # Gold layer: customer summary, monthly trend, category breakdown & leaderboard tables
```

## How It Works

### 1. Synthetic transaction generation (`synthetic_population.py`)
Builds a static pool of 2,500 synthetic customers, each assigned an income tier (`Value`, `Mass`, `Premium`, `Ultra`) weighted to reflect South Africa's income distribution, a lifestyle archetype (e.g. Commuter, Foodie), and account attributes (opening balance, minimum balance floor, monthly income) drawn from tier-specific ranges.

It then runs an infinite real-time loop (can be stopped manually) that emits card-spend, decline, and deposit events as individual JSON files, with:
- **Live South African time (SAST)** driving realistic peak/off-peak traffic pacing (lunch rush, evening commute, off-peak hours) and real merchant trading-hour checks (e.g. no clothing purchases at 3am, liquor sales restrictions).
- A merchant registry across categories (groceries, fuel, dining, retail, transport, fitness, pharmacy, domestic travel, alcohol & nightlife, utilities) with category- and merchant-specific spend distributions per income tier.
- Persisted checkpointing, so the generator can resume its event counter after a restart.

Events are written to a Unity Catalog Volume landing path for the bronze layer to pick up.

### 2. Bronze layer — raw ingestion (`micro_batch_ingestion.py`, `bronze.sql`)
Ingests the raw JSON files into a Delta table via Databricks Autoloader (`cloudFiles`) / `read_files`, in append-only mode with a defined event schema (transaction, customer, and merchant details). No transformation is applied — this is the untransformed system of record for every event as it arrived, including source file lineage.

### 3. Greenbacks rules configuration (`delta_rules_table.sql`)
Seeds the business rules the silver layer applies, modeled on Nedbank's Greenbacks Rewards Programme guide:
- **Config constants** - Greenback-to-Rand conversion rate, and a bp fuel bonus rate (25c/litre).
- **Level-based earn rates** - cashback earn rate by card class and customer level.
- **Customer level mapping** - `living_tier` used as a proxy for a customer's Greenbacks level, since the synthetic data has no debit orders, loans, or savings accounts to derive a real behavioral-goal level from.
- **Category eligibility rules** - which spend categories earn points by default (most do; fuel is excluded by default, income/deposits never earn).
- **Merchant-level overrides** - e.g. BP Express is the sole bp-equivalent fuel merchant eligible for both ordinary cashback and the extra fuel bonus.

### 4. Silver layer — cleaning, rules, and points (`silver.py`)
- Performs an incremental, watermark-based load of new bronze rows (using the silver table's own max ingestion time, with a safety buffer — no separate checkpoint file needed).
- Flattens nested customer/merchant JSON structures and deduplicates by `transaction_id`.
- Joins each transaction against the Greenbacks rule tables to compute `points_earned` and `points_value_rand`.
- Recomputes a validated **cumulative points balance per customer** via a window function over the full table (not incremental — a status change on an old transaction must ripple through every later cumulative total for that customer).
- Includes a built-in schema-mismatch auto-reset safeguard and a post-run assertion that cumulative totals reconcile against an independently computed sum, failing loudly rather than shipping silently inconsistent data.

### 5. Gold layer — data mart (`data_mart.py`)
Builds four aggregate tables purely from the already-validated silver layer (no re-derivation of business logic):
- `gold_customer_points_summary` — one row per customer: lifetime spend, lifetime points earned/value, transaction counts, latest known profile attributes. The "rewards dashboard" view.
- `gold_monthly_points_trend` - points earned, value, and spend per customer per month.
- `gold_category_breakdown` - spend and points earned per customer per category, for personalized insights (e.g. "you could earn more by spending at bp").
- `gold_category_leaderboard` - category-level totals across all customers, for an exec/dashboard view of which spend categories drive the most rewards payout.

A final consistency check cross-validates that lifetime totals in the customer summary table reconcile exactly with the monthly trend roll-up, and fails the run if gold has drifted from silver.

## Tech Stack
- **Databricks** (Delta Lake, Unity Catalog, Autoloader, Declarative/Streaming Tables)
- **PySpark** - Structured Streaming, DataFrame API, window functions
- **SQL** - streaming table definitions, rules/config tables
- **Python** - synthetic data generation (Faker, live SAST-aware simulation logic)

## Known Limitations
- **Customer level is a proxy.** A real Greenbacks level is recalculated monthly from five behavioral goals (salary deposits, digital transaction count, debit orders, savings/investment growth, loan repayments). This generator has no debit order, loan, or savings account data to derive that from, so `living_tier` is used as an explicit, clearly-labeled stand-in.
- **Card class is fixed.** All simulated cards are treated as `VISA_MASTERCARD_DEBIT`, since they debit an account balance rather than draw on a credit line; no Amex or credit card behavior is modeled.
- **Reference fuel price is a static placeholder**, not a live market rate.

## Getting Started
1. Run `synthetic_population.py` in a Databricks notebook/cluster to start the live event feed writing to the configured Unity Catalog Volume landing path.
2. Run `bronze.sql` (or `micro_batch_ingestion.py`) to stream raw events into the bronze Delta table.
3. Run `delta_rules_table.sql` once to seed the Greenbacks configuration and rules tables.
4. Run `silver.py` to clean, apply rules, and calculate points.
5. Run `data_mart.py` to build the gold-layer reporting tables.

> Note: table paths in these scripts reference specific Unity Catalog volumes/schemas (e.g. `rewards_catalog.loyalty.*`, `workspace.default.*`) — update these to match your own Databricks workspace before running.
