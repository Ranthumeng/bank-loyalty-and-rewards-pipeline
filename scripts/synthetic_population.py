import json
import random
import time
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from faker import Faker
import os

# All "what time is it right now" business logic (peak/off-peak traffic, merchant
# trading hours) is evaluated in South African local time, not whatever timezone the
# host machine happens to be running in (e.g. a Databricks cluster running in UTC).
# Event timestamps written to the JSON payload stay in UTC (the standard for
# downstream storage/Delta Lake), but every "is it open / is it lunchtime" decision
# converts through this tz first so the simulation reflects real live SAST time.
SAST = ZoneInfo("Africa/Johannesburg")

def sa_now():
    """Returns the current instant as a (utc_datetime, local_sast_datetime) pair,
    always derived live from the real system clock -- never cached, never simulated --
    so every call reflects the actual moment the code is running."""
    now_utc = datetime.now(timezone.utc)
    return now_utc, now_utc.astimezone(SAST)

# Define the tracking and raw landing target folders inside Unity Catalog volume
LANDING_VOLUME_PATH = "/Volumes/rewards_catalog/loyalty/landing/card_spend_landing"
STATE_VOLUME_PATH   = "/Volumes/rewards_catalog/loyalty/landing/checkpoint_state"
STATE_FILE_PATH     = os.path.join(STATE_VOLUME_PATH, "stream_metadata.json")

# Ensure required persistent tracking paths exist before initiating calculations
os.makedirs(LANDING_VOLUME_PATH, exist_ok=True)
os.makedirs(STATE_VOLUME_PATH, exist_ok=True)


# ==========================================
# STATE PERSISTENCE
# ==========================================
def load_persisted_counter():
    """Reads the last recorded event counter tracking index from UC Volume storage."""
    if os.path.exists(STATE_FILE_PATH):
        try:
            with open(STATE_FILE_PATH, "r") as f:
                state_data = json.load(f)
                return state_data.get("last_event_id", 0)
        except Exception as e:
            print(f"[WARN] State tracker found but could not be parsed: {e}. Starting fresh.")
            return 0
    return 0

def save_persisted_counter(current_count):
    """Writes the current loop sequence counter progress state back to UC Volume storage."""
    try:
        state_payload = {
            "last_event_id": current_count,
            "last_updated_ts": datetime.now(timezone.utc).isoformat()
        }
        with open(STATE_FILE_PATH, "w") as f:
            json.dump(state_payload, f)
    except Exception as e:
        print(f"[WARN] System failed to save sequence tracking progress: {e}")



# ==========================================
# INITIALIZATION
# ==========================================

# Initialize Faker with South African locale
try:
    fake = Faker('zu_ZA')
except Exception:
    try:
        fake = Faker()
    except Exception:
        fake = None


# ==========================================
# 1. STRUCTURAL DATA LOOKUPS & REGISTRIES
# ==========================================

MERCHANT_REGISTRY = {
    "groceries": [
        {"name": "Shoprite", "mcc": "5411"},
        {"name": "Boxer Superstores", "mcc": "5411"},
        {"name": "Cambridge Food", "mcc": "5411"},
        {"name": "USave", "mcc": "5411"},
        {"name": "Pick n Pay", "mcc": "5411"},
        {"name": "Spar", "mcc": "5411"},
        {"name": "Checkers", "mcc": "5411"},
        {"name": "Woolworths Food", "mcc": "5411"},
        {"name": "Food Lovers Market", "mcc": "5411"}
    ],
    "fuel": [
        {"name": "Sasol", "mcc": "5541"},
        {"name": "TotalEnergies", "mcc": "5541"},
        {"name": "Shell Select", "mcc": "5541"},
        {"name": "Caltex FreshStop", "mcc": "5541"},
        {"name": "Engen QuickShop", "mcc": "5541"},
        {"name": "BP Express", "mcc": "5541"}
    ],
    "utilities": [
        {"name": "City of Joburg Municipality", "mcc": "4900"},
        {"name": "City of Cape Town Water/Elec", "mcc": "4900"},
        {"name": "Eskom Direct Prepaid", "mcc": "4900"},
        {"name": "Tshwane Metropolitan Council", "mcc": "4900"},
        {"name": "eThekwini Municipality", "mcc": "4900"},
        {"name": "PayCity Utilities Portal", "mcc": "4900"}
    ],
    "dining": [
        {"name": "KFC", "mcc": "5814"},
        {"name": "Debonairs Pizza", "mcc": "5814"},
        {"name": "Hungry Lion", "mcc": "5814"},
        {"name": "Spur Steak Ranches", "mcc": "5812"},
        {"name": "Nandos", "mcc": "5814"},
        {"name": "Wimpy", "mcc": "5814"},
        {"name": "Roman's Pizza", "mcc": "5814"},
        {"name": "Tasha's Cafe", "mcc": "5812"},
        {"name": "The Grillhouse", "mcc": "5812"},
        {"name": "Tiger's Milk", "mcc": "5812"},
        {"name": "Starbucks South Africa", "mcc": "5814"},
        {"name": "Steers", "mcc": "5814"},           
        {"name": "Mugg & Bean", "mcc": "5812"},      
        {"name": "Ocean Basket", "mcc": "5812"},     
        {"name": "Pedros Chicken", "mcc": "5814"},   
        {"name": "Fish and Chip Co", "mcc": "5814"}, 
        {"name": "RocoMamas", "mcc": "5812"},        
        {"name": "Chesanyama", "mcc": "5814"} 
    ],
    "pharmacy": [
        {"name": "MediRite Pharmacy", "mcc": "5912"},
        {"name": "Clicks", "mcc": "5912"},
        {"name": "Dis-Chem", "mcc": "5912"},
    ],
    "retail": [
        {"name": "PEP Stores", "mcc": "5311"},
        {"name": "Ackermans", "mcc": "5311"},
        {"name": "Jet", "mcc": "5311"},
        {"name": "Mr Price", "mcc": "5311"},
        {"name": "Foschini", "mcc": "5311"},
        {"name": "Truworths", "mcc": "5311"},
        {"name": "Takealot Online", "mcc": "5311"},
        {"name": "Zara", "mcc": "5311"},
        {"name": "Cape Union Mart", "mcc": "5311"},
        {"name": "Superbalist", "mcc": "5311"},
        {"name": "Bash Online", "mcc": "5311"}, 
    ],
    "fitness": [
        {"name": "Local Community Gym", "mcc": "7997"},
        {"name": "Mr Price Sport", "mcc": "5941"},
        {"name": "Planet Fitness", "mcc": "7997"},
        {"name": "Viva Gym", "mcc": "7997"},
        {"name": "Totalsports", "mcc": "5941"},
        {"name": "Virgin Active", "mcc": "7997"},
        {"name": "Sportsmans Warehouse", "mcc": "5941"},
        {"name": "Cycle Lab", "mcc": "5941"}
    ],
    "transport": [
        {"name": "PRASA Metrorail", "mcc": "4111"},
        {"name": "Uber South Africa", "mcc": "4121"},
        {"name": "Bolt Ride", "mcc": "4121"},
        {"name": "Gautrain", "mcc": "4111"}
    ],
    "domestic_travel": [
        {"name": "FlySafair", "mcc": "4511"},       
        {"name": "Airlink", "mcc": "4511"},           
        {"name": "South African Airways", "mcc": "4511"}, 
        {"name": "Lift Airline", "mcc": "4511"},      
        {"name": "Sanparks Accommodation", "mcc": "7011"} 
    ],
    "alcohol_and_nightlife": [
        {"name": "Tops at Spar", "mcc": "5921"},     
        {"name": "LiquorShop Checkers", "mcc": "5921"}, 
        {"name": "Pick n Pay Liquor", "mcc": "5921"},
        {"name": "Ultra Liquors", "mcc": "5921"}, 
    ]
}

BRAND_TIERS = {
    "groceries": {
        "Premium":  ["Woolworths Food", "Checkers", "Food Lovers Market"],
        "Mass":     ["Pick n Pay", "Spar", "Shoprite"],
        "Value":    ["Boxer Superstores", "USave", "Cambridge Food"],
        "Ultra":    ["Woolworths Food", "Checkers", "Food Lovers Market"]
    },
    "fuel": {
        "Premium":  ["BP Express", "Shell Select"],
        "Mass":     ["Sasol", "TotalEnergies", "Engen QuickShop"],
        "Value":    ["Caltex FreshStop"],
        "Ultra":    ["BP Express", "Shell Select"]
    },
    "dining": {
        "Premium":  ["The Grillhouse", "Tasha's Cafe", "Tiger's Milk"],
        "Mass":     ["Spur Steak Ranches", "Nandos", "Mugg & Bean", "Ocean Basket", 
                 "Starbucks South Africa", "RocoMamas"],
        "Value":    ["KFC", "Debonairs Pizza", "Hungry Lion", "Wimpy", "Roman's Pizza", 
                  "Steers", "Pedros Chicken", "Fish and Chip Co", "Chesanyama"],
        "Ultra":    ["The Grillhouse", "Tasha's Cafe", "Tiger's Milk"]
    },
    "pharmacy": {
        "Premium":  ["Dis-Chem"],
        "Mass":     ["Clicks"],
        "Value":    ["MediRite Pharmacy"],
        "Ultra":    ["Dis-Chem"]
    },
    "retail": {
        "Premium":  ["Zara", "Cape Union Mart", "Superbalist", "Takealot Online"],
        "Mass":     ["Mr Price", "Foschini", "Truworths", "Bash Online"],
        "Value":    ["PEP Stores", "Ackermans", "Jet"],
        "Ultra":    ["Zara", "Cape Union Mart", "Superbalist", "Takealot Online"]
    },
    "fitness": {
        "Premium":  ["Virgin Active", "Cycle Lab", "Sportsmans Warehouse"],
        "Mass":     ["Planet Fitness", "Viva Gym", "Totalsports"],
        "Value":    ["Local Community Gym", "Mr Price Sport"],
        "Ultra":    ["Virgin Active", "Cycle Lab", "Sportsmans Warehouse"]
    },
    "transport": {
        "Premium":  ["Uber South Africa"],
        "Mass":     ["Gautrain", "Bolt Ride"],
        "Value":    ["PRASA Metrorail"],
        "Ultra":    ["Uber South Africa"]
    },
    "domestic_travel": {
        # Value tier is intentionally absent: TIER_MATRIX excludes domestic_travel
        # entirely for LOW_INCOME (real households at that income level essentially
        # never fly domestically), and no Value-eligible archetype includes this
        # category, so this tier is never looked up in practice.
        "Mass":     ["FlySafair", "Sanparks Accommodation"],
        "Premium":  ["Airlink", "South African Airways", "Sanparks Accommodation"],
        "Ultra":    ["Lift Airline", "South African Airways"]
    },
    "alcohol_and_nightlife": {
        "Value":    ["Pick n Pay Liquor", "Tops at Spar"],
        "Mass":     ["Tops at Spar", "Pick n Pay Liquor", "LiquorShop Checkers"],
        "Premium":  ["LiquorShop Checkers", "Ultra Liquors"],
        # "Ultra Liquors" here is just the name of a real liquor store chain -- it isn't
        # related to our "Ultra" income tier, it just happens to also fit that bracket.
        "Ultra":    ["Ultra Liquors"]
    }
    # "utilities" has no entry here by design. Municipal/utility billing doesn't have a
    # "premium vs budget" brand tier the way retail does -- everyone in an area pays the
    # same municipality. That merchant is instead resolved directly from the customer's
    # own home municipality (see anchors["utilities_merchant"] and the override in
    # generate_card_spend_event), so a BRAND_TIERS lookup for it would be unused anyway.
}
ARCHETYPE_RULES = {
    "Commuter": {
        # Most commuters rely on taxis/trains/rideshare (transport), but a meaningful
        # minority drive their own car and pay for fuel directly.
        "categories":       ["transport", "groceries", "fuel"],
        "weights":          [0.50, 0.35, 0.15], 
        "loyalty_strength": 0.75
    },
    "Foodie": {
        "categories":       ["dining", "alcohol_and_nightlife", "groceries"], 
        "weights":          [0.50, 0.25, 0.25],
        "loyalty_strength": 0.45  
    },
    "Primary Residential Consumer": {
        # Everyday household spend includes the odd pharmacy run (medication, toiletries).
        "categories":   ["groceries", "retail", "utilities", "pharmacy"], 
        "weights":      [0.50, 0.27, 0.13, 0.10],
        # Highly routine: same neighborhood grocer, same municipality bill, month after month.
        "loyalty_strength": 0.70
    },
    "Fitness Enthusiast": {
        # Health-conscious spenders also buy supplements/health products at pharmacies.
        "categories":   ["groceries", "fitness", "utilities", "pharmacy"], 
        "weights":      [0.50, 0.32, 0.10, 0.08],
        # Habitual by nature (same gym, same routine) but slightly less anchored than a
        # residential consumer since gym/fitness retail involves more occasional variety.
        "loyalty_strength": 0.65
    },
    "High-Frequency Domestic Traveler": {
        "categories":   ["domestic_travel", "retail", "dining"], 
        "weights":      [0.60, 0.20, 0.20],
        # Frequent flyer habits (same airline for miles) are offset by inherent variety --
        # different cities mean different restaurants and retail on each trip.
        "loyalty_strength": 0.50
    }
}

# Archetype-to-tier mapping 
ARCHETYPE_TIERS = {
    "High-Frequency Domestic Traveler": "Premium",  
    "Fitness Enthusiast": "Premium",  
    "Foodie": "Mass",
    "Commuter": "Mass",  
    "Primary Residential Consumer": "Value"
}
# COMPREHENSIVE AREA REGISTRY (From First Version - All 9 Provinces)
AREA_REGISTRY = {
    "Premium": [
        {"area": "Sandton", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Bryanston", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Waterkloof", "province": "Gauteng", "municipality": "City of Tshwane Metropolitan Municipality"},
        {"area": "Camps Bay", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Constantia", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Stellenbosch Winelands Estate", "province": "Western Cape", "municipality": "Stellenbosch Local Municipality"},
        {"area": "Umhlanga", "province": "KwaZulu-Natal", "municipality": "eThekwini Metropolitan Municipality"},
        {"area": "Ballito", "province": "KwaZulu-Natal", "municipality": "KwaDukuza Local Municipality"},
        {"area": "Walmer", "province": "Eastern Cape", "municipality": "Nelson Mandela Bay Metropolitan Municipality"},
        {"area": "Beacon Bay", "province": "Eastern Cape", "municipality": "Buffalo City Metropolitan Municipality"},
        {"area": "Waverley", "province": "Free State", "municipality": "Mangaung Metropolitan Municipality"},
        {"area": "Bendor", "province": "Limpopo", "municipality": "Polokwane Local Municipality"},
        {"area": "Steiltes", "province": "Mpumalanga", "municipality": "City of Mbombela Local Municipality"},
        {"area": "Cashan", "province": "North West", "municipality": "Rustenburg Local Municipality"},
        {"area": "Monument Heights", "province": "Northern Cape", "municipality": "Sol Plaatje Local Municipality"},
    ],
    "Mass": [
        {"area": "Randburg", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Kempton Park", "province": "Gauteng", "municipality": "City of Ekurhuleni Metropolitan Municipality"},
        {"area": "Centurion", "province": "Gauteng", "municipality": "City of Tshwane Metropolitan Municipality"},
        {"area": "Bellville", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Durbanville", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Westville", "province": "KwaZulu-Natal", "municipality": "eThekwini Metropolitan Municipality"},
        {"area": "Pinetown", "province": "KwaZulu-Natal", "municipality": "eThekwini Metropolitan Municipality"},
        {"area": "Summerstrand", "province": "Eastern Cape", "municipality": "Nelson Mandela Bay Metropolitan Municipality"},
        {"area": "Vincent", "province": "Eastern Cape", "municipality": "Buffalo City Metropolitan Municipality"},
        {"area": "Bayswater", "province": "Free State", "municipality": "Mangaung Metropolitan Municipality"},
        {"area": "Fauna Park", "province": "Limpopo", "municipality": "Polokwane Local Municipality"},
        {"area": "West Acres", "province": "Mpumalanga", "municipality": "City of Mbombela Local Municipality"},
        {"area": "Safarituine", "province": "North West", "municipality": "Rustenburg Local Municipality"},
        {"area": "Hadison Park", "province": "Northern Cape", "municipality": "Sol Plaatje Local Municipality"},
        {"area": "Vanderbijlpark", "province": "Gauteng", "municipality": "Emfuleni Local Municipality"},
    ],
    "Value": [
        {"area": "Soweto", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Tembisa", "province": "Gauteng", "municipality": "City of Ekurhuleni Metropolitan Municipality"},
        {"area": "Mamelodi", "province": "Gauteng", "municipality": "City of Tshwane Metropolitan Municipality"},
        {"area": "Khayelitsha", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Mitchells Plain", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Umlazi", "province": "KwaZulu-Natal", "municipality": "eThekwini Metropolitan Municipality"},
        {"area": "KwaMashu", "province": "KwaZulu-Natal", "municipality": "eThekwini Metropolitan Municipality"},
        {"area": "Motherwell", "province": "Eastern Cape", "municipality": "Nelson Mandela Bay Metropolitan Municipality"},
        {"area": "Mdantsane", "province": "Eastern Cape", "municipality": "Buffalo City Metropolitan Municipality"},
        {"area": "Botshabelo", "province": "Free State", "municipality": "Mangaung Metropolitan Municipality"},
        {"area": "Seshego", "province": "Limpopo", "municipality": "Polokwane Local Municipality"},
        {"area": "KaNyamazane", "province": "Mpumalanga", "municipality": "City of Mbombela Local Municipality"},
        {"area": "Tlhabane", "province": "North West", "municipality": "Rustenburg Local Municipality"},
        {"area": "Galeshewe", "province": "Northern Cape", "municipality": "Sol Plaatje Local Municipality"},
        {"area": "Thabong", "province": "Free State", "municipality": "Matjhabeng Local Municipality"},
    ],
    # Ultra-high-net-worth households are not evenly spread across the country the way
    # the other tiers are -- they cluster tightly into a handful of nodes (northern
    # Johannesburg, the Cape Town Atlantic Seaboard/Southern Suburbs, and the KZN North
    # Coast). Modeling this as a small, geographically concentrated set is more realistic
    # than forcing all-9-province coverage.
    "Ultra": [
        {"area": "Steyn City", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Dainfern", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Hyde Park", "province": "Gauteng", "municipality": "City of Johannesburg Metropolitan Municipality"},
        {"area": "Clifton", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Bishopscourt", "province": "Western Cape", "municipality": "City of Cape Town Metropolitan Municipality"},
        {"area": "Val de Vie Estate", "province": "Western Cape", "municipality": "Drakenstein Local Municipality"},
        {"area": "Zimbali Coastal Estate", "province": "KwaZulu-Natal", "municipality": "KwaDukuza Local Municipality"},
    ],
}


# ==========================================
# TIER-BASED SPENDING MATRIX
# ==========================================
TIER_MATRIX = {
    "LOW_INCOME": {
        "groceries":                {"beta": 110, "min_amt": 30.00, "max_amt": 600.00},
        "fuel": None,  # Low-income households unlikely to own cars
        "utilities":                {"beta": 120, "min_amt": 30.00, "max_amt": 500.00},
        "dining":                   {"beta": 50, "min_amt": 15.00, "max_amt": 300.00},
        "retail":                   {"beta": 80, "min_amt": 20.00, "max_amt": 800.00},
        "domestic_travel": None,  # Low-income households rarely fly domestically
        "fitness":                  {"beta": 60, "min_amt": 20.00, "max_amt": 500.00},
        "pharmacy": None,  # Public healthcare is free 
        "transport":                {"beta": 80, "min_amt": 10.00, "max_amt": 300.00},  # Uber/Bolt (taxis are cash-only, excluded)
        "alcohol_and_nightlife":    {"beta": 50, "min_amt": 15.00, "max_amt": 350.00}
    },
    "MIDDLE_CLASS": {
        "groceries":                {"beta": 450, "min_amt": 150.00, "max_amt": 2500.00},
        "fuel":                     {"beta": 400, "min_amt": 150.00, "max_amt": 1600.00},
        "utilities":                {"beta": 600, "min_amt": 250.00, "max_amt": 2500.00},
        "dining":                   {"beta": 220, "min_amt": 50.00, "max_amt": 1500.00},
        "retail":                   {"beta": 350, "min_amt": 100.00, "max_amt": 4500.00},
        "domestic_travel":          {"beta": 900, "min_amt": 300.00, "max_amt": 9500.00},
        "fitness":                  {"beta": 280, "min_amt": 80.00, "max_amt": 2000.00},
        "pharmacy":                 {"beta": 180, "min_amt": 50.00, "max_amt": 1200.00},
        "transport":                {"beta": 250, "min_amt": 50.00, "max_amt": 1500.00},
        "alcohol_and_nightlife":    {"beta": 200, "min_amt": 80.00, "max_amt": 1800.00}
    },
    "AFFLUENT": {
        "groceries":                {"beta": 850, "min_amt": 300.00, "max_amt": 5500.00},
        "fuel":                     {"beta": 650, "min_amt": 300.00, "max_amt": 2400.00},
        "utilities":                {"beta": 1500, "min_amt": 800.00, "max_amt": 6500.00},
        "dining":                   {"beta": 650, "min_amt": 200.00, "max_amt": 5000.00},
        "retail":                   {"beta": 1200, "min_amt": 300.00, "max_amt": 25000.00},
        "domestic_travel":          {"beta": 4500, "min_amt": 1500.00, "max_amt": 55000.00},
        "fitness":                  {"beta": 950, "min_amt": 300.00, "max_amt": 8000.00},
        "pharmacy":                 {"beta": 550, "min_amt": 200.00, "max_amt": 6500.00},
        "transport":                {"beta": 800, "min_amt": 200.00, "max_amt": 5000.00},
        "alcohol_and_nightlife":    {"beta": 750, "min_amt": 300.00, "max_amt": 12000.00}
    },
    "ULTRA_HIGH": {
        "groceries":                {"beta": 1600, "min_amt": 600.00, "max_amt": 12000.00},
        "fuel":                     {"beta": 950, "min_amt": 500.00, "max_amt": 3500.00},
        "utilities":                {"beta": 3800, "min_amt": 2000.00, "max_amt": 22000.00},
        "dining":                   {"beta": 2100, "min_amt": 500.00, "max_amt": 25000.00},
        "retail":                   {"beta": 5500, "min_amt": 1000.00, "max_amt": 120000.00},
        "domestic_travel":          {"beta": 18000, "min_amt": 5000.00, "max_amt": 450000.00},
        "fitness":                  {"beta": 2800, "min_amt": 800.00, "max_amt": 25000.00},
        "pharmacy":                 {"beta": 1600, "min_amt": 500.00, "max_amt": 35000.00},
        "transport":                {"beta": 2200, "min_amt": 600.00, "max_amt": 18000.00},
        "alcohol_and_nightlife":    {"beta": 3500, "min_amt": 1000.00, "max_amt": 85000.00}
    }
    
}


# Map ARCHETYPE_TIERS to TIER_MATRIX keys
TIER_MAPPING = {
    "Value": "LOW_INCOME",
    "Mass": "MIDDLE_CLASS",
    "Premium": "AFFLUENT",
    "Ultra": "ULTRA_HIGH"
}


# ==========================================
# PAYMENT ENTRY-MODE DISTRIBUTIONS
# ==========================================
# How a transaction gets entered (tap, chip+pin, or online) is a property of the
# CATEGORY, not a fixed global rate -- a municipal bill and a grocery run don't get
# paid the same way. Weights are [TAP_AND_GO, CHIP_PIN, ONLINE], illustrative rather
# than official statistics, and reflect typical South African payment behavior:
ENTRY_MODE_LABELS = ["TAP_AND_GO", "CHIP_PIN", "ONLINE"]
CATEGORY_ENTRY_MODE_WEIGHTS = {
    "groceries":               [65, 30, 5],   # contactless is now the default at SA supermarket tills
    "fuel":                    [15, 75, 10],  # attendant-served pumps still lean heavily on chip+pin
    "utilities":               [5, 15, 80],   # municipal accounts are paid via EFT/online banking, not tapped
    "dining":                  [55, 30, 15],  # some online share from delivery apps
    "retail":                  [55, 30, 15],  # in-store still dominant, e-commerce a meaningful minority
    "fitness":                 [55, 35, 10],
    "transport":               [70, 25, 5],   # Gautrain/PRASA tap-card ticketing; Uber/Bolt are overridden below
    "domestic_travel":         [5, 15, 80],   # flights/accommodation are booked online, not swiped
    "pharmacy":                [55, 35, 10],
    "alcohol_and_nightlife":   [55, 35, 10],
}

# Specific merchants that are ALWAYS one payment channel regardless of category norms:
# ride-hailing apps are paid in-app, and these retailers are online-only storefronts
# with no physical till to tap or dip a card at.
ONLINE_ONLY_MERCHANTS = {"Uber South Africa", "Bolt Ride", "Takealot Online", "Bash Online", "Superbalist"}


# ==========================================
# MERCHANT OPERATING HOURS
# ==========================================
# Trading hours in South Africa, evaluated against real live local (SAST) time as the
# script runs -- a customer can't "spend" at a clothing store at 3am, and off-consumption
# liquor sales are legally restricted (closed Sundays, shorter Saturday hours). Hours
# below are illustrative typical SA trading windows, not a specific chain's exact hours.
#
# "always_open": True covers categories/merchants that genuinely trade 24/7 in practice:
# fuel stations (most SA forecourts run 24 hours), utilities/EFT payments (online banking
# has no closing time), ride-hailing apps, and pure e-commerce storefronts.
MERCHANT_OPERATING_HOURS = {
    "groceries":              {"open": 7,  "close": 21},   # typical supermarket hours
    "fuel":                   {"always_open": True},        # SA forecourts widely trade 24/7
    "utilities":              {"always_open": True},        # EFT / online municipal payment
    "dining":                 {"open": 8,  "close": 23},
    "pharmacy":               {"open": 8,  "close": 20},
    "retail":                 {"open": 9,  "close": 19},
    "fitness":                {"open": 5,  "close": 22},
    "transport":              {"open": 5,  "close": 23},    # default; ride-hailing overridden below
    "domestic_travel":        {"always_open": True},        # flights/accommodation booked online anytime
    "alcohol_and_nightlife":  {"open": 9,  "close": 18},    # off-consumption liquor sales; special-cased below
}

# Per-merchant overrides where a specific brand doesn't follow its category default
# (ride-hailing apps and pure online storefronts trade 24/7 regardless of category norms;
# rail operators keep to scheduled operating windows tighter than the transport default).
MERCHANT_HOURS_OVERRIDE = {
    "Uber South Africa":    {"always_open": True},
    "Bolt Ride":            {"always_open": True},
    "Takealot Online":      {"always_open": True},
    "Bash Online":          {"always_open": True},
    "Superbalist":          {"always_open": True},
    "PRASA Metrorail":      {"open": 5, "close": 20},
    "Gautrain":             {"open": 5, "close": 21},
}


def is_merchant_open(category, merchant_name, now_local):
    """Checks whether a given merchant is currently trading, based on real live SAST
    local time (now_local). This is what makes spend generation 'real-time' rather than
    just timestamp-labeled -- a 2am transaction attempt at a clothing store won't be
    generated, because the clothing store genuinely isn't open at 2am."""
    hours = MERCHANT_HOURS_OVERRIDE.get(merchant_name) or MERCHANT_OPERATING_HOURS.get(category)

    if not hours or hours.get("always_open"):
        return True

    weekday = now_local.weekday()  # Monday=0 ... Sunday=6

    if category == "alcohol_and_nightlife":
        # SA off-consumption liquor trading: closed Sundays entirely, shorter hours
        # on Saturdays (13:00 cutoff), normal window on weekdays.
        if weekday == 6:
            return False
        close_hour = 13 if weekday == 5 else hours["close"]
        return hours["open"] <= now_local.hour < close_hour

    return hours["open"] <= now_local.hour < hours["close"]


# ==========================================
# CORE SYSTEM ALGORITHMS & GENERATORS
# ==========================================

class MockFaker:
    def city(self): 
        return random.choice(["Johannesburg", "Cape Town", "Durban", "Pretoria", "Soweto", "Gqeberha"])
    def uuid4(self): 
        return f"{random.randint(1000,9999)}a-{random.randint(1000,9999)}b"
    class UniqueMock:
        def random_int(self, min, max): 
            return random.randint(min, max)
    def __init__(self): 
        self.unique = self.UniqueMock()

if not fake:
    fake = MockFaker()

# LUHN CHECKSUM FOR SA ID (From First Version)
def calculate_luhn_checksum(id_12_digits):
    """Calculates the valid 13th digit of a South African ID using the Luhn algorithm."""
    digits = [int(d) for d in id_12_digits]
    for i in range(len(digits) - 1, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    total = sum(digits)
    return str((10 - (total % 10)) % 10)

# SA ID GENERATOR
def generate_south_african_id(age):
    """Generates a structurally flawless 13-digit South African ID number."""
    current_year = datetime.now().year
    birth_year = current_year - age
    yy = f"{birth_year % 100:02d}"
    mm = f"{random.randint(1, 12):02d}"
    
    # Calendar month-end safety bounds
    if mm == "02":
        dd = f"{random.randint(1, 28):02d}"
    elif mm in ["04", "06", "09", "11"]:
        dd = f"{random.randint(1, 30):02d}"
    else:
        dd = f"{random.randint(1, 31):02d}"
        
    gender = f"{random.randint(0, 9999):04d}"
    citizenship = str(random.choice([0, 1]))
    race_digit = "8"
    
    base_12_digits = f"{yy}{mm}{dd}{gender}{citizenship}{race_digit}"
    checksum_digit = calculate_luhn_checksum(base_12_digits)
    
    return f"{base_12_digits}{checksum_digit}"

# ==========================================
# CUSTOMER POOL BUILDER
# ==========================================
def build_customer_pool(size=2500):
    """Creates a static pool of loyalty program participants with anchored habits."""
    pool = []
    archetypes = list(ARCHETYPE_RULES.keys())

    # Define which archetypes are valid for each tier
    TIER_VALID_ARCHETYPES = {
        "Value": ["Foodie", "Primary Residential Consumer"],  # No fuel, no flights
        "Mass": ["Commuter", "Foodie", "Primary Residential Consumer"],
        "Premium": archetypes,  # All archetypes available
        "Ultra": archetypes     # All archetypes available; weighting below favors the
                                 # same archetypes that are natural fits for Premium
                                 # (frequent flyers, fitness-focused spenders)
    }

    # Balance and income profiles by tier. Illustrative defaults, not sourced statistics
    # -- reasoned from South Africa's highly unequal income distribution (Gini ~0.63,
    # among the highest in the world) the same way the tier population weights below are.
    # min_balance_floor is drawn per-customer within a tier range (not a single fixed
    # number) so two Value-tier customers can have meaningfully different amounts of
    # breathing room, same as real accounts do.
    ACCOUNT_PROFILE_BY_TIER = {
        "Value": {
            "opening_balance": (150, 4500),
            "min_balance_floor": (0, 250),
            "monthly_income": (1500, 12000),
        },
        "Mass": {
            "opening_balance": (800, 25000),
            "min_balance_floor": (0, 1200),
            "monthly_income": (9000, 45000),
        },
        "Premium": {
            "opening_balance": (15000, 180000),
            "min_balance_floor": (5000, 25000),
            "monthly_income": (40000, 180000),
        },
        "Ultra": {
            "opening_balance": (250000, 5000000),
            "min_balance_floor": (100000, 1000000),
            "monthly_income": (250000, 2500000),
        }
    }

    for _ in range(size):
        age = random.randint(18, 65)

        # Draw economic tier FIRST, weighted to reflect South Africa's highly unequal
        # income distribution.
        customer_tier = random.choices(
            population=["Value", "Mass", "Premium", "Ultra"],
            weights=[0.55, 0.32, 0.12, 0.01],
            k=1
        )[0]

        valid_archetypes_for_tier = TIER_VALID_ARCHETYPES[customer_tier]
        natural_home_tier = "Premium" if customer_tier == "Ultra" else customer_tier
        archetype_weights = [
            3 if ARCHETYPE_TIERS.get(a) == natural_home_tier else 1
            for a in valid_archetypes_for_tier
        ]
        archetype = random.choices(valid_archetypes_for_tier, weights=archetype_weights, k=1)[0]

        # Pick a home area consistent with that tier, spanning all provinces
        home_area = random.choice(AREA_REGISTRY[customer_tier])

        # Establish a persistent "Home Category" derived from archetype choice
        category_choices = ARCHETYPE_RULES[archetype]["categories"]
        category_weights = ARCHETYPE_RULES[archetype]["weights"]
        home_category = random.choices(category_choices, weights=category_weights, k=1)[0]

        # Anchor a specific, persistent "Home Merchant" they visit constantly
        tier_brands = BRAND_TIERS.get(home_category, {}).get(customer_tier)
        if tier_brands:
            matching = [m for m in MERCHANT_REGISTRY[home_category] if m["name"] in tier_brands]
            home_merchant = random.choice(matching) if matching else random.choice(MERCHANT_REGISTRY[home_category])
        else:
            home_merchant = random.choice(MERCHANT_REGISTRY[home_category])

        # Utilities merchant is derived directly from the customer's actual municipality
        utilities_merchant = {"name": home_area["municipality"], "mcc": "4900"}

        # Build local neighborhood merchants for each category
        local_neighborhood_merchants = {}
        for cat, merchants in MERCHANT_REGISTRY.items():
            tier_filtered = [m for m in merchants if m["name"] in BRAND_TIERS.get(cat, {}).get(customer_tier, [])]
            pool_to_sample = tier_filtered if tier_filtered else merchants
            local_neighborhood_merchants[cat] = random.sample(pool_to_sample, min(2, len(pool_to_sample)))

        account_profile = ACCOUNT_PROFILE_BY_TIER[customer_tier]
        opening_balance = round(random.uniform(*account_profile["opening_balance"]), 2)
        min_balance_floor = round(random.uniform(*account_profile["min_balance_floor"]), 2)
        monthly_income = round(random.uniform(*account_profile["monthly_income"]), 2)

        pool.append({
            "customer_id": f"CUST-{fake.unique.random_int(min=100000, max=999999)}",
            "demographics": {
                "age": age,
                "sa_id_number": generate_south_african_id(age),
                "home_town": home_area["area"],
                "home_province": home_area["province"],
                "living_tier": customer_tier,
            },
            "lifestyle_archetype": archetype,
            "account_profile": {
                "opening_balance": opening_balance,
                "current_balance": opening_balance,
                "min_balance_floor": min_balance_floor,
                "monthly_income": monthly_income,
                "payday": random.randint(1, 28),  # day-of-month; capped at 28 so it's valid in every month
                "last_paid_period": None,          # (year, month) tuple once the first payday deposit lands
            },
            "profile_anchors": {
                "home_category": home_category,
                "preferred_home_merchant": home_merchant,
                "utilities_merchant": utilities_merchant,
                "local_neighborhood_merchants": local_neighborhood_merchants
            }
        })

    return pool

# ==========================================
# SPORADIC DEPOSIT EVENTS (cash, transfers, refunds)
# ==========================================
# SALARY is deliberately NOT one of these sources -- it's handled as a guaranteed
# once-a-month payday deposit inline in generate_card_spend_event() instead (see below).
# Mixing a guaranteed recurring paycheck into the same random draw as an occasional cash
# deposit would understate how reliably salaries actually arrive relative to how random
# a cash deposit or refund genuinely is.
DEPOSIT_SOURCE_WEIGHTS = {
    "Value":   [40, 35, 25],
    "Mass":    [35, 40, 25],
    "Premium": [20, 55, 25],
    "Ultra":   [10, 65, 25]
}
DEPOSIT_SOURCES = ["CASH_DEPOSIT", "TRANSFER_IN", "REFUND"]

def generate_deposit_event():
    """Generates a standalone, sporadic credit event -- cash deposits, person-to-person
    transfers, and merchant refunds. Called independently by the main loop for texture,
    not as a side effect of a declined spend (that coupling was the original bug: it
    meant a spend that couldn't go through silently turned into an unrelated income
    event instead of being recorded as what it actually was -- a decline)."""
    customer = random.choice(CUSTOMER_POOL)
    tier = customer["demographics"]["living_tier"]
    profile = customer["account_profile"]

    source = random.choices(
        DEPOSIT_SOURCES,
        weights=DEPOSIT_SOURCE_WEIGHTS.get(tier, [40, 35, 25]),
        k=1
    )[0]

    if source == "CASH_DEPOSIT":
        amount = round(random.uniform(200, profile["monthly_income"] * 0.20), 2)
        merchant_name = "Cash Deposit"
    elif source == "TRANSFER_IN":
        amount = round(random.uniform(100, profile["monthly_income"] * 0.25), 2)
        merchant_name = "Bank Transfer"
    else:
        amount = round(random.uniform(50, profile["monthly_income"] * 0.10), 2)
        merchant_name = "Merchant Refund"

    account_balance_before = profile["current_balance"]
    profile["current_balance"] = round(profile["current_balance"] + amount, 2)

    return {
        "transaction_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "currency": "ZAR",
        "transaction_type": "DEPOSIT",
        "direction": "CREDIT",
        "status": "APPROVED",
        "source": source,
        "customer": {
            "customer_id": customer["customer_id"],
            "lifestyle_archetype": customer["lifestyle_archetype"],
            "living_tier": tier,
            "home_town": customer["demographics"]["home_town"]
        },
        "merchant": {
            "name": merchant_name,
            "mcc": "6012",
            "category": "income"
        },
        "account_balance_before": account_balance_before,
        "account_balance_after": profile["current_balance"]
    }

def _pick_open_category_and_merchant(customer, tier_profile, now_local, max_attempts=8):
    """Selects a (category, merchant) pair for this spend attempt that is BOTH
    economically valid for the customer's tier (tier_profile[category] is not None)
    AND actually trading right now, per real live SAST time. This replaces a plain
    'is the category available at this income tier' check with 'is the category
    available at this income tier AND is that specific merchant open at this instant' --
    the same archetype-loyalty-then-fallback pattern as before, just time-aware."""
    archetype = customer["lifestyle_archetype"]
    anchors = customer["profile_anchors"]
    category_choices = ARCHETYPE_RULES[archetype]["categories"]
    category_weights = ARCHETYPE_RULES[archetype]["weights"]
    home_category = anchors["home_category"]
    home_merchant_probability = ARCHETYPE_RULES[archetype]["loyalty_strength"]

    for attempt in range(max_attempts):
        if attempt == 0 and random.random() < home_merchant_probability:
            candidate_category = home_category
            candidate_merchant = anchors["preferred_home_merchant"]
        else:
            candidate_category = random.choices(category_choices, weights=category_weights, k=1)[0]
            candidate_merchant = random.choice(anchors["local_neighborhood_merchants"][candidate_category])

        if tier_profile.get(candidate_category) is None:
            continue  # not economically available at this customer's income tier
        if not is_merchant_open(candidate_category, candidate_merchant["name"], now_local):
            continue  # merchant is closed right now

        return candidate_category, candidate_merchant

    # Exhaustive fallback: scan every category valid at this tier for ANY merchant
    # that's open right now, so a real-time-open transaction can still be found even
    # if the archetype's usual haunts are all closed at this hour.
    open_options = []
    for cat, config in tier_profile.items():
        if config is None:
            continue
        for merchant in MERCHANT_REGISTRY.get(cat, []):
            if is_merchant_open(cat, merchant["name"], now_local):
                open_options.append((cat, merchant))
    if open_options:
        return random.choice(open_options)

    # Absolute last resort (should essentially never trigger, since fuel, utilities,
    # transport's Uber/Bolt leg, and domestic_travel booking are always-open): utilities
    # is always-open and always economically valid, so it's a safe universal fallback.
    return "utilities", anchors["utilities_merchant"]


def generate_card_spend_event():
    """Generates a transaction where behavior is driven by customer archetype, economic
    tier, real live South African local time (for merchant trading hours and payday
    timing), and available balance. Returns a DECLINE instead of a SPEND if the
    attempted amount is more than the customer's available balance -- the transaction is
    recorded as having been attempted and refused, and the balance is left untouched,
    rather than silently being swapped for an unrelated deposit event."""
    customer = random.choice(CUSTOMER_POOL)
    tier = customer["demographics"]["living_tier"]
    profile = customer["account_profile"]
    anchors = customer["profile_anchors"]

    # Live clock read: UTC instant for the stored timestamp, SAST local time for every
    # real-world business-hours decision (payday date, merchant trading hours).
    now_utc, now_local = sa_now()

    # Guaranteed monthly salary, independent of whether THIS specific spend attempt
    # below succeeds or fails. Income arriving should never be conditional on the
    # customer almost running out of money first -- that's backwards, and was the
    # bug in generate_deposit_event() previously being called as a decline-rescue.
    # Keyed off local SAST calendar date, since a salary "lands on the 25th" refers to
    # the 25th in South African local time, not the UTC calendar date.
    current_period = (now_local.year, now_local.month)
    if now_local.day == profile["payday"] and profile["last_paid_period"] != current_period:
        profile["current_balance"] = round(profile["current_balance"] + profile["monthly_income"], 2)
        profile["last_paid_period"] = current_period

    # Map customer tier to TIER_MATRIX and get category-specific spending parameters
    tier_profile_key = TIER_MAPPING.get(tier, "MIDDLE_CLASS")
    tier_profile = TIER_MATRIX[tier_profile_key]

    # Pick a category/merchant that's both economically valid for this tier AND
    # actually open right now, in real SAST time.
    selected_category, merchant_info = _pick_open_category_and_merchant(customer, tier_profile, now_local)
    category_config = tier_profile[selected_category]

    # Utilities are billed by the customer's own municipality, not a random
    # nationwide utilities merchant -- override whatever the selection above picked.
    if selected_category == "utilities":
        merchant_info = anchors["utilities_merchant"]

    # Generate transaction amount scaled by their economic status and category
    base_amount = random.gammavariate(alpha=2, beta=category_config["beta"])
    amount = round(base_amount, 2)

    # Force bounding box constraints
    amount = max(category_config["min_amt"], min(amount, category_config["max_amt"]))

    # Entry mode depends on the category's typical payment channel, with specific
    # online-only merchants (ride-hailing apps, online-only retailers) always forced
    # to ONLINE regardless of what their category normally looks like.
    # (Taxis are excluded entirely from MERCHANT_REGISTRY since they're cash-only
    # and would never generate a card transaction in the first place.)
    if merchant_info["name"] in ONLINE_ONLY_MERCHANTS:
        entry_mode = "ONLINE"
    else:
        weights = CATEGORY_ENTRY_MODE_WEIGHTS.get(selected_category, [60, 30, 10])
        entry_mode = random.choices(ENTRY_MODE_LABELS, weights=weights, k=1)[0]

    card_type = random.choices(["VISA", "MASTERCARD"], weights=[50, 50], k=1)[0]

    # Balance check -- this is where a DECLINE branches off from a normal SPEND.
    # A transaction is only declined if it costs more than what's actually in the
    # account right now -- min_balance_floor is not used as a decline trigger, since
    # a transaction the customer can genuinely afford should never be refused just for
    # dipping below some notional comfort buffer.
    account_balance_before = profile["current_balance"]
    projected_balance = round(profile["current_balance"] - amount, 2)

    base_event = {
        "transaction_id": fake.uuid4(),
        "timestamp": now_utc.isoformat(),
        "amount": amount,
        "currency": "ZAR",
        "card_type": card_type,
        "entry_mode": entry_mode,
        "direction": "DEBIT",
        "customer": {
            "customer_id": customer["customer_id"],
            "lifestyle_archetype": customer["lifestyle_archetype"],
            "living_tier": tier,
            "home_town": customer["demographics"]["home_town"]
        },
        "merchant": {
            "name": merchant_info["name"],
            "mcc": merchant_info["mcc"],
            "category": selected_category
        },
    }

    if amount > account_balance_before:
        # DECLINED: the transaction costs more than what's in the account, so nothing
        # was actually debited and the balance is left untouched (before == after).
        # The attempted amount/category/merchant are still recorded -- decline rate by
        # category/tier is itself a genuinely useful feature, not something to throw away.
        base_event.update({
            "transaction_type": "DECLINE",
            "status": "DECLINED",
            "decline_reason": "INSUFFICIENT_FUNDS",
            "account_balance_before": account_balance_before,
            "account_balance_after": account_balance_before,
        })
        return base_event

    # APPROVED: debit the balance for real.
    profile["current_balance"] = projected_balance
    base_event.update({
        "transaction_type": "SPEND",
        "status": "APPROVED",
        "account_balance_before": account_balance_before,
        "account_balance_after": profile["current_balance"],
    })
    return base_event

# ==========================================
# 6. INITIALIZATION & STREAMING LOOP
# ==========================================
print("--- INITIALIZING REALISTIC REAL-TIME ENVIRONMENT ---")


# Build the customer static profile pool
CUSTOMER_POOL = build_customer_pool(2500) 


print(f"Successfully generated database pool with {len(CUSTOMER_POOL)} distinct customer entities.")
print(f"Sample Verification: {CUSTOMER_POOL[0]['customer_id']} | "
      f"ID: {CUSTOMER_POOL[0]['demographics']['sa_id_number']} | "
      f"Town: {CUSTOMER_POOL[0]['demographics']['home_town']}")
print("="*115 + "\n")


print("Starting Production-Grade Peak/Off-Peak Card Feed... Click standard Jupyter STOP button to halt.")
print(f"Live clock reference: South Africa Standard Time (Africa/Johannesburg, UTC+2)")
print("-" * 125)


event_counter = load_persisted_counter()  # Resume from last checkpoint

# Probability that a given loop tick generates a standalone deposit event (cash deposit,
# transfer, refund) instead of a spend/decline attempt. Guaranteed monthly salary is
# handled separately, inline in generate_card_spend_event() -- this is just sporadic
# "money in" texture layered on top of that.
DEPOSIT_EVENT_PROBABILITY = 0.08


try:
    while True:
        event_counter += 1

        # Live clock read for THIS tick's peak/off-peak traffic classification.
        # Always converted through SAST so the "lunch rush" / "evening commute" bands
        # line up with real South African wall-clock time regardless of what timezone
        # the underlying host machine's system clock is set to.
        _, now_local = sa_now()
        hour = now_local.hour
        
        # Generate the next event -- usually a spend attempt (which may itself resolve
        # to an APPROVED spend or a DECLINE, and which now also respects real-time
        # merchant trading hours), occasionally a standalone deposit.
        if random.random() < DEPOSIT_EVENT_PROBABILITY:
            tx = generate_deposit_event()
        else:
            tx = generate_card_spend_event()
        
        # Evaluate traffic velocity thresholds dynamically matching live SAST clock hours
        if 0 <= hour <= 5:
            traffic_density = random.uniform(0.01, 0.05)
            status_tag = "OFF-PEAK (LATE NIGHT)"
        elif 12 <= hour <= 14:
            traffic_density = random.uniform(0.85, 1.00)
            status_tag = "HIGH PEAK (LUNCH RUSH)"
        elif 16 <= hour <= 19:
            traffic_density = random.uniform(0.80, 0.95)
            status_tag = "HIGH PEAK (EVENING COMMUTE)"
        elif 6 <= hour <= 9:
            traffic_density = random.uniform(0.40, 0.65)
            status_tag = "MORNING RAMP"
        else:
            traffic_density = random.uniform(0.20, 0.45)
            status_tag = "MID-DAY STABLE"


        # Translate numerical system density into dynamic inverse delay pacing
        base_sleep = 1.5 / (traffic_density + 0.01)
        actual_delay = max(0.05, min(base_sleep, 8.0)) + random.uniform(-0.05, 0.2)
        
        # Output tabular real-time business log visualization. Not every event type has
        # the same fields -- deposits have no entry_mode/card_type, declines have an
        # entry_mode but no successful debit -- so build the trailing detail per type
        # instead of assuming every event looks like a SPEND.
        # tx['timestamp'] is stored in UTC (correct for the JSON payload / downstream
        # Delta Lake use), but the console log should read in real South African local
        # time -- otherwise the printed clock looks 2 hours "behind" the merchant-hours
        # and payday logic, which are already evaluated against true SAST time.
        event_ts_local = datetime.fromisoformat(tx['timestamp']).astimezone(SAST)
        clean_ts = event_ts_local.strftime("%Y-%m-%d %H:%M:%S")

        if tx['transaction_type'] == "DEPOSIT":
            trailing_detail = f"via {tx['source']}"
        else:
            trailing_detail = f"via {tx['entry_mode']}"

        status_flag = "" if tx['status'] == "APPROVED" else f" [{tx['status']}: {tx.get('decline_reason', '')}]"

        print(f"[{clean_ts}] | {status_tag:<26} | EVT-{event_counter:06d} | "
              f"{tx['customer']['customer_id']} ({tx['customer']['living_tier']:<12}) -> "
              f"R{tx['amount']:>9.2f} | "
              f"{tx['merchant']['name']:<22} | {trailing_detail}{status_flag}")
        
        # Write to Unity Catalog Volume for down-stream Delta Lake Streaming
        try:
            # Write transaction as JSON file (one file per event for streaming)
            file_path = f"{LANDING_VOLUME_PATH}/tx_{event_counter:08d}.json"
            with open(file_path, 'w') as f:
                json.dump(tx, f)
            
            # Save checkpoint every 100 events
            if event_counter % 100 == 0:
                save_persisted_counter(event_counter)
                print(f"[CHECKPOINT] Saved state at event {event_counter}")
                
        except Exception as e:
            print(f"[ERROR] Failed to write transaction to Volume: {e}")


        # Pacing pause for loop control
        time.sleep(actual_delay)


except KeyboardInterrupt:
    print("\nTime-slice consumer streaming feed disconnected.")
    save_persisted_counter(event_counter)
    print(f"Final checkpoint saved at event {event_counter}")
