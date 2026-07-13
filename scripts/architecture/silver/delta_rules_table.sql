-- ==========================================
-- GREENBACKS RULE TABLES
-- Modeled on Nedbank's Greenbacks Rewards Programme 
-- ==========================================

-- Global config constants from the guide: Greenback-to-Rand conversion rate GB1 = R0.028
-- fuel spend into litres for the bp 25c/litre reward.
-- fuel price is a static placeholder for project purposes
CREATE TABLE IF NOT EXISTS workspace.default.greenbacks_config (
  config_key STRING,
  config_value DOUBLE
) USING DELTA;

INSERT INTO workspace.default.greenbacks_config VALUES
  ('gb_value_rand', 0.028),                 -- 1 Greenback = R0.028
  ('bp_fuel_reward_rand_per_litre', 0.25),  -- 25c/litre at bp
  ('reference_fuel_price_per_litre', 23.50); -- placeholder

-- cashback earn rate by card class and level. We only ever generate
-- VISA_MASTERCARD_DEBIT in this pipeline, but the
-- full table is seeded here for fidelity to the source and in case credit
-- modeling gets added later.
CREATE TABLE IF NOT EXISTS workspace.default.greenbacks_level_earn_rates (
  level INT,
  card_class STRING,   -- 'AMEX_DEBIT_CREDIT' | 'VISA_MASTERCARD_CREDIT' | 'VISA_MASTERCARD_DEBIT'
  earn_rate DOUBLE      -- stored as a decimal fraction, e.g. 0.001 = 0.1%, NOT a whole percent
) USING DELTA;

INSERT INTO catalog_name.schema_name.greenbacks_level_earn_rates VALUES  
  (1, 'VISA_MASTERCARD_CREDIT', 0.002),      -- Points 
  (1, 'VISA_MASTERCARD_DEBIT',  0.001),      -- earned 
  (2, 'VISA_MASTERCARD_CREDIT', 0.004),      -- differ
  (2, 'VISA_MASTERCARD_DEBIT',  0.002),      -- at      
  (3, 'VISA_MASTERCARD_CREDIT', 0.006),      -- different 
  (3, 'VISA_MASTERCARD_DEBIT',  0.003),      -- levels 
  (4, 'VISA_MASTERCARD_CREDIT', 0.008),
  (4, 'VISA_MASTERCARD_DEBIT',  0.004),       
  (5, 'VISA_MASTERCARD_CREDIT', 0.01),
  (5, 'VISA_MASTERCARD_DEBIT',  0.005);       
  
-- Customer Greenbacks Level -- PROXY ONLY. Real levels are recalculated monthly from
-- 5 behavioral goals (salary deposits, digital transaction count, debit order count,
-- savings/investment growth, loan repayment history 
-- I don't know how to simulate debit orders, loans, or savings accounts in a realistic way
-- so a real level can't be computed from this data. living_tier is used here as an explicit,
-- clearly-labeled stand-in
CREATE TABLE IF NOT EXISTS workspace.default.greenbacks_customer_level (
  living_tier STRING,
  level INT
) USING DELTA;

INSERT INTO catalog_name.schema_name.greenbacks_customer_level VALUES
  ('Value',   1),
  ('Mass',    2),
  ('Premium', 4),
  ('Ultra',   5);

-- exclusion list:
--   - "Fuel purchases not made at bp stations" are explicitly excluded -> fuel
--     defaults to NOT eligible; 
--     even though raw EFTs and
--     utilities is eligible=true.
--   - Everything else (groceries, dining, retail, fitness, transport,
--     domestic_travel, pharmacy, alcohol_and_nightlife) is ordinary card spend with
--     no carve-out in the guide, so it defaults to eligible.
CREATE TABLE IF NOT EXISTS workspace.default.greenbacks_category_rules (
  category STRING,
  eligible BOOLEAN
) USING DELTA;

INSERT INTO catalog_name.schema_name.greenbacks_category_rules VALUES
  ('groceries',             true),
  ('fuel',                  false),  -- default: only bp-equivalent merchants earn (see overrides)
  ('utilities',             true),   -- modeled as Bill Payments, which is allowed
  ('dining',                true),
  ('retail',                true),
  ('domestic_travel',       true),
  ('fitness',               true),
  ('pharmacy',              true),
  ('transport',             true),
  ('alcohol_and_nightlife', true),
  ('income',                false);  -- deposits/refunds -- not a purchase

-- Merchant-level overrides take precedence over the category default above.
-- BP Express is our only bp-equivalent fuel merchant: it's eligible for
-- ordinary card cashback (unlike other fuel brands) AND gets the extra 25c/litre
CREATE TABLE IF NOT EXISTS workspace.default.greenbacks_merchant_overrides (
  merchant_name STRING,
  eligible BOOLEAN,
  bp_fuel_bonus BOOLEAN
) USING DELTA;

INSERT INTO workspace.default.greenbacks_merchant_overrides VALUES
  ('BP Express', true, true);

