#!/usr/bin/env python3
"""
Retail Sales Data Mart - end-to-end pipeline (Python 3, standard library only)

Stages:  EXTRACT (CSV -> staging)  ->  TRANSFORM (clean/validate/derive)
         ->  LOAD (star schema: 4 dimensions + 1 fact)
         ->  CUBE (all cuboids materialised)  ->  OLAP (roll-up, drill-down,
         slice, dice, pivot)  ->  reports exported as CSV.

Usage:
    python retail_datamart_pipeline.py                 # full run (creates sample source if missing)
    python retail_datamart_pipeline.py --rows 500      # bigger generated sample
    python retail_datamart_pipeline.py --source my.csv # use your own source file
    python retail_datamart_pipeline.py --query "SELECT region, SUM(sales_amount) FROM v_sales GROUP BY region"
"""
import argparse, csv, datetime, itertools, logging, os, random, sqlite3, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_COLS = ["txn_id", "txn_date", "product_code", "product_name", "category", "brand",
            "store_code", "store_name", "city", "region", "cust_id", "cust_name",
            "segment", "qty", "unit_price", "discount"]
MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
log = logging.getLogger("pipeline")

# --------------------------------------------------------------------------
# 0. SOURCE GENERATION (stand-in for the retail POS system)
# --------------------------------------------------------------------------
def generate_source(path, rows=300, seed=7):
    random.seed(seed)
    prods = [("P01", "Basmati Rice 5kg", "Grocery", "FarmFresh", 420), ("P02", "Sunflower Oil 1L", "Grocery", "GoldDrop", 150),
             ("P03", "Tea 500g", "Grocery", "ChaiCo", 260), ("P04", "T-Shirt", "Apparel", "UrbanWear", 499),
             ("P05", "Jeans", "Apparel", "UrbanWear", 1299), ("P06", "Sneakers", "Apparel", "StepUp", 2199),
             ("P07", "Smartphone", "Electronics", "Techno", 14999), ("P08", "Earbuds", "Electronics", "Techno", 1999),
             ("P09", "Mixer Grinder", "Electronics", "HomeEase", 3499), ("P10", "Notebook Pack", "Stationery", "PaperPlus", 180)]
    stores = [("S1", "Pune Central", "Pune", "West"), ("S2", "Mumbai Bay", "Mumbai", "West"),
              ("S3", "Delhi Hub", "Delhi", "North"), ("S4", "Chennai Mart", "Chennai", "South")]
    custs = [("C1", "Asha", "Gold"), ("C2", "Ravi", "Silver"), ("C3", "Meera", "Gold"),
             ("C4", "Kiran", "Bronze"), ("C5", "Sana", "Silver"), ("C6", "Dev", "Bronze")]
    d0 = datetime.date(2024, 1, 1)
    out = []
    for t in range(1, rows + 1):
        d = d0 + datetime.timedelta(days=random.randint(0, 729))
        p, s, c = random.choice(prods), random.choice(stores), random.choice(custs)
        name = p[1].lower() if t % 9 == 0 else p[1]          # inconsistent casing to be cleaned
        out.append([t, d.strftime("%d-%m-%Y"), p[0], name, p[2], p[3], s[0], s[1], s[2], s[3],
                    c[0], c[1], c[2], random.randint(1, 4), p[4], random.choice([0, 0, 5, 10])])
    # deliberately dirty records to demonstrate data-quality handling
    out.append([rows + 1, "15-03-2024", "P01", "Basmati Rice 5kg", "Grocery", "FarmFresh", "S1", "Pune Central", "Pune", "West", "C1", "Asha", "Gold", -2, 420, 0])
    out.append([rows + 2, "20-05-2024", "P03", "Tea 500g", "Grocery", "ChaiCo", "S2", "Mumbai Bay", "Mumbai", "West", "C2", "Ravi", "Silver", "", 260, 0])
    out.append([5, "11-11-2024", "P02", "Sunflower Oil 1L", "Grocery", "GoldDrop", "S3", "Delhi Hub", "Delhi", "North", "C3", "Meera", "Gold", 2, 150, 0])
    out.append([rows + 3, "31-02-2024", "P04", "T-Shirt", "Apparel", "UrbanWear", "S1", "Pune Central", "Pune", "West", "C4", "Kiran", "Bronze", 1, 499, 0])
    out.append([rows + 4, "10-06-2024", "P99", "Unknown", "Grocery", "X", "S1", "Pune Central", "Pune", "West", "C4", "Kiran", "Bronze", 1, 100, 150])
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(SRC_COLS); w.writerows(out)
    log.info("Generated source file %s (%d rows)", path, len(out))

# --------------------------------------------------------------------------
# 1. DDL
# --------------------------------------------------------------------------
DDL = """
DROP VIEW  IF EXISTS v_sales;
DROP TABLE IF EXISTS fact_sales;   DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;  DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_customer; DROP TABLE IF EXISTS stg_sales;
DROP TABLE IF EXISTS etl_rejects;  DROP TABLE IF EXISTS cube_sales;
DROP TABLE IF EXISTS etl_audit;

CREATE TABLE stg_sales(txn_id TEXT, txn_date TEXT, product_code TEXT, product_name TEXT, category TEXT, brand TEXT,
  store_code TEXT, store_name TEXT, city TEXT, region TEXT, cust_id TEXT, cust_name TEXT, segment TEXT,
  qty TEXT, unit_price TEXT, discount TEXT);
CREATE TABLE etl_rejects(txn_id TEXT, reason TEXT, raw_row TEXT);
CREATE TABLE etl_audit(run_ts TEXT, extracted INT, rejected INT, loaded INT);

CREATE TABLE dim_date(date_key INTEGER PRIMARY KEY, full_date TEXT, day INT, month INT, month_name TEXT,
  quarter TEXT, year INT, day_of_week TEXT, is_weekend INT);
CREATE TABLE dim_product(product_key INTEGER PRIMARY KEY AUTOINCREMENT, product_code TEXT UNIQUE,
  product_name TEXT, category TEXT, brand TEXT);
CREATE TABLE dim_store(store_key INTEGER PRIMARY KEY AUTOINCREMENT, store_code TEXT UNIQUE,
  store_name TEXT, city TEXT, region TEXT);
CREATE TABLE dim_customer(customer_key INTEGER PRIMARY KEY AUTOINCREMENT, cust_id TEXT UNIQUE,
  cust_name TEXT, segment TEXT);
CREATE TABLE fact_sales(sales_key INTEGER PRIMARY KEY AUTOINCREMENT, txn_id INT UNIQUE,
  date_key INT NOT NULL, product_key INT NOT NULL, store_key INT NOT NULL, customer_key INT NOT NULL,
  quantity INT, unit_price REAL, discount_pct REAL, sales_amount REAL,
  FOREIGN KEY(date_key) REFERENCES dim_date(date_key), FOREIGN KEY(product_key) REFERENCES dim_product(product_key),
  FOREIGN KEY(store_key) REFERENCES dim_store(store_key), FOREIGN KEY(customer_key) REFERENCES dim_customer(customer_key));
CREATE INDEX ix_fact_date ON fact_sales(date_key);   CREATE INDEX ix_fact_prod ON fact_sales(product_key);
CREATE INDEX ix_fact_store ON fact_sales(store_key); CREATE INDEX ix_fact_cust ON fact_sales(customer_key);

CREATE VIEW v_sales AS
SELECT f.txn_id, d.full_date, d.year, d.quarter, d.month, d.month_name, p.product_code, p.product_name, p.category, p.brand,
       s.store_name, s.city, s.region, c.cust_name, c.segment, f.quantity, f.unit_price, f.discount_pct, f.sales_amount
FROM fact_sales f JOIN dim_date d ON f.date_key=d.date_key JOIN dim_product p ON f.product_key=p.product_key
JOIN dim_store s ON f.store_key=s.store_key JOIN dim_customer c ON f.customer_key=c.customer_key;
"""

# --------------------------------------------------------------------------
# 2. EXTRACT
# --------------------------------------------------------------------------
def extract(db, source):
    with open(source, newline="") as f:
        rd = csv.DictReader(f)
        missing = set(SRC_COLS) - set(rd.fieldnames or [])
        if missing: raise SystemExit(f"Source file missing columns: {sorted(missing)}")
        rows = [tuple((r[c] or "").strip() for c in SRC_COLS) for r in rd]
    db.executemany("INSERT INTO stg_sales VALUES(%s)" % ",".join("?" * len(SRC_COLS)), rows)
    log.info("EXTRACT  : %d rows read from %s into stg_sales", len(rows), os.path.basename(source))
    return len(rows)

# --------------------------------------------------------------------------
# 3. TRANSFORM (validate + clean + derive)  -> yields clean dicts / reject tuples
# --------------------------------------------------------------------------
def transform(db):
    clean, rejects, seen = [], [], set()
    valid_prod = {r[0] for r in db.execute("SELECT DISTINCT product_code FROM stg_sales WHERE product_name <> 'Unknown'")}
    for row in db.execute("SELECT * FROM stg_sales").fetchall():
        r = dict(zip(SRC_COLS, row))
        def rej(reason): rejects.append((r["txn_id"], reason, "|".join(row)))
        if not r["txn_id"].isdigit():                 rej("invalid txn_id"); continue
        if r["txn_id"] in seen:                       rej("duplicate txn_id"); continue
        if r["qty"] == "":                            rej("missing quantity"); continue
        try: qty = int(r["qty"]); price = float(r["unit_price"]); disc = float(r["discount"] or 0)
        except ValueError:                            rej("non-numeric measure"); continue
        if qty <= 0:                                  rej("quantity <= 0"); continue
        if price <= 0:                                rej("price <= 0"); continue
        if not 0 <= disc <= 100:                      rej("discount out of range"); continue
        if r["product_code"] not in valid_prod:       rej("unknown product"); continue
        try: d = datetime.datetime.strptime(r["txn_date"], "%d-%m-%Y").date()
        except ValueError:                            rej("invalid date"); continue
        seen.add(r["txn_id"])
        clean.append(dict(txn_id=int(r["txn_id"]), date=d, product_code=r["product_code"],
                          product_name=r["product_name"].title() if r["product_name"].islower() else r["product_name"],
                          category=r["category"].title(), brand=r["brand"], store_code=r["store_code"], store_name=r["store_name"],
                          city=r["city"].title(), region=r["region"].title(), cust_id=r["cust_id"], cust_name=r["cust_name"].title(),
                          segment=r["segment"].title(), qty=qty, price=price, disc=disc,
                          amount=round(qty * price * (1 - disc / 100), 2)))
    log.info("TRANSFORM: %d clean, %d rejected", len(clean), len(rejects))
    return clean, rejects

# --------------------------------------------------------------------------
# 4. LOAD
# --------------------------------------------------------------------------
def load(db, clean, rejects):
    db.executemany("INSERT INTO etl_rejects VALUES(?,?,?)", rejects)
    for r in clean:
        d = r["date"]; dk = int(d.strftime("%Y%m%d"))
        db.execute("INSERT OR IGNORE INTO dim_date VALUES(?,?,?,?,?,?,?,?,?)",
                   (dk, d.isoformat(), d.day, d.month, MONTHS[d.month], f"Q{(d.month-1)//3+1}", d.year, d.strftime("%A"), int(d.weekday() >= 5)))
        db.execute("INSERT OR IGNORE INTO dim_product(product_code,product_name,category,brand) VALUES(?,?,?,?)",
                   (r["product_code"], r["product_name"], r["category"], r["brand"]))
        db.execute("INSERT OR IGNORE INTO dim_store(store_code,store_name,city,region) VALUES(?,?,?,?)",
                   (r["store_code"], r["store_name"], r["city"], r["region"]))
        db.execute("INSERT OR IGNORE INTO dim_customer(cust_id,cust_name,segment) VALUES(?,?,?)",
                   (r["cust_id"], r["cust_name"], r["segment"]))
        pk = db.execute("SELECT product_key FROM dim_product WHERE product_code=?", (r["product_code"],)).fetchone()[0]
        sk = db.execute("SELECT store_key FROM dim_store WHERE store_code=?", (r["store_code"],)).fetchone()[0]
        ck = db.execute("SELECT customer_key FROM dim_customer WHERE cust_id=?", (r["cust_id"],)).fetchone()[0]
        db.execute("INSERT INTO fact_sales(txn_id,date_key,product_key,store_key,customer_key,quantity,unit_price,discount_pct,sales_amount) "
                   "VALUES(?,?,?,?,?,?,?,?,?)", (r["txn_id"], dk, pk, sk, ck, r["qty"], r["price"], r["disc"], r["amount"]))
    log.info("LOAD     : %d facts loaded; dims -> date=%d product=%d store=%d customer=%d", len(clean),
             *[db.execute(f"SELECT COUNT(*) FROM dim_{t}").fetchone()[0] for t in ("date", "product", "store", "customer")])

def verify(db, n_extracted, n_loaded, n_rejected, expected_total):
    """Post-load integrity checks - pipeline fails loudly if any check fails."""
    assert n_extracted == n_loaded + n_rejected, "row reconciliation failed"
    orphans = db.execute("""SELECT COUNT(*) FROM fact_sales f LEFT JOIN dim_date d ON f.date_key=d.date_key
        LEFT JOIN dim_product p ON f.product_key=p.product_key LEFT JOIN dim_store s ON f.store_key=s.store_key
        LEFT JOIN dim_customer c ON f.customer_key=c.customer_key
        WHERE d.date_key IS NULL OR p.product_key IS NULL OR s.store_key IS NULL OR c.customer_key IS NULL""").fetchone()[0]
    assert orphans == 0, "orphan fact rows found"
    src = round(expected_total, 2)   # independent total computed from the cleaned records
    tgt = db.execute("SELECT ROUND(SUM(sales_amount),2) FROM fact_sales").fetchone()[0]
    assert abs((src or 0) - (tgt or 0)) < 1, f"amount mismatch transformed={src} fact={tgt}"
    log.info("VERIFY   : reconciliation OK (extracted=%d = loaded %d + rejected %d), no orphans, totals match (%.2f)",
             n_extracted, n_loaded, n_rejected, tgt)

# --------------------------------------------------------------------------
# 5. DATA CUBE  - every cuboid (2^n group-bys) over n dimensions is materialised
# --------------------------------------------------------------------------
CUBE_DIMS = ["year", "region", "category", "segment"]
def build_cube(db):
    db.execute("CREATE TABLE cube_sales(year TEXT, region TEXT, category TEXT, segment TEXT, "
               "total_sales REAL, units INT, txns INT, cuboid TEXT)")
    n = 0
    for k in range(len(CUBE_DIMS) + 1):
        for combo in itertools.combinations(CUBE_DIMS, k):
            sel = ", ".join(c if c in combo else f"'ALL' AS {c}" for c in CUBE_DIMS)
            grp = f"GROUP BY {', '.join(combo)}" if combo else ""
            db.execute(f"INSERT INTO cube_sales SELECT {sel}, ROUND(SUM(sales_amount),2), SUM(quantity), COUNT(*), "
                       f"'{'+'.join(combo) or 'APEX'}' FROM v_sales {grp}")
            n += 1
    cells = db.execute("SELECT COUNT(*) FROM cube_sales").fetchone()[0]
    log.info("CUBE     : %d cuboids materialised (2^%d), %d cells in cube_sales", n, len(CUBE_DIMS), cells)

# --------------------------------------------------------------------------
# 6. OLAP OPERATIONS
# --------------------------------------------------------------------------
def q(db, sql, args=()):
    cur = db.execute(sql, args); return [d[0] for d in cur.description], cur.fetchall()

def rollup(db):          # Quarter -> Year -> Grand total
    return q(db, """SELECT year, quarter, ROUND(SUM(sales_amount),2) total_sales FROM v_sales GROUP BY year, quarter
        UNION ALL SELECT year, 'ALL', ROUND(SUM(sales_amount),2) FROM v_sales GROUP BY year
        UNION ALL SELECT 'ALL','ALL', ROUND(SUM(sales_amount),2) FROM v_sales ORDER BY 1,2""")

def drilldown(db, year):  # Year -> Quarter -> Month
    return q(db, "SELECT quarter, month_name, ROUND(SUM(sales_amount),2) total_sales FROM v_sales WHERE year=? "
                 "GROUP BY quarter, month, month_name ORDER BY month", (year,))

def slice_(db, category):  # fix ONE dimension
    return q(db, "SELECT region, year, ROUND(SUM(sales_amount),2) total_sales FROM v_sales WHERE category=? "
                 "GROUP BY region, year ORDER BY 1,2", (category,))

def dice(db, year, regions, categories):  # restrict SEVERAL dimensions
    rp, cp = ",".join("?" * len(regions)), ",".join("?" * len(categories))
    return q(db, f"SELECT city, category, ROUND(SUM(sales_amount),2) total_sales, SUM(quantity) units FROM v_sales "
                 f"WHERE year=? AND region IN ({rp}) AND category IN ({cp}) GROUP BY city, category ORDER BY 1,2",
             (year, *regions, *categories))

def pivot(db, year):      # rotate: categories become columns
    cats = [r[0] for r in db.execute("SELECT DISTINCT category FROM v_sales ORDER BY 1")]
    cols = ", ".join(f"ROUND(SUM(CASE WHEN category='{c}' THEN sales_amount END),2) AS {c}" for c in cats)
    return q(db, f"SELECT region, {cols} FROM v_sales WHERE year=? GROUP BY region ORDER BY 1", (year,))

def top_products(db, n=3):
    return q(db, "SELECT product_name, ROUND(SUM(sales_amount),2) total_sales, SUM(quantity) units FROM v_sales "
                 "GROUP BY product_name ORDER BY 2 DESC LIMIT ?", (n,))

def yoy(db):
    return q(db, """SELECT region, ROUND(SUM(CASE WHEN year=2024 THEN sales_amount END),2) y2024,
        ROUND(SUM(CASE WHEN year=2025 THEN sales_amount END),2) y2025,
        ROUND(100.0*(SUM(CASE WHEN year=2025 THEN sales_amount END)-SUM(CASE WHEN year=2024 THEN sales_amount END))
        /SUM(CASE WHEN year=2024 THEN sales_amount END),1) growth_pct FROM v_sales GROUP BY region ORDER BY 1""")

def cube_lookup(db):     # read straight from the materialised cube (fast path)
    return q(db, "SELECT year, region, total_sales, units FROM cube_sales WHERE cuboid='year+region' ORDER BY 1,2")

# --------------------------------------------------------------------------
# 7. OUTPUT HELPERS
# --------------------------------------------------------------------------
def show(title, res, outdir=None, fname=None):
    cols, rows = res
    print(f"\n=== {title} ===")
    w = [max(len(str(c)), *(len(f"{r[i]:,.2f}" if isinstance(r[i], float) else str(r[i])) for r in rows)) if rows else len(str(c)) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(w[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * x for x in w))
    for r in rows:
        print("  ".join((f"{v:,.2f}" if isinstance(v, float) else str(v)).ljust(w[i]) for i, v in enumerate(r)))
    if outdir and fname:
        with open(os.path.join(outdir, fname), "w", newline="") as f:
            wr = csv.writer(f); wr.writerow(cols); wr.writerows(rows)

# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Retail sales data mart pipeline")
    ap.add_argument("--source", default=os.path.join(HERE, "sales_source.csv"))
    ap.add_argument("--db", default=os.path.join(HERE, "sales_datamart.db"))
    ap.add_argument("--out", default=os.path.join(HERE, "reports"))
    ap.add_argument("--rows", type=int, default=300, help="rows to generate if source file is absent")
    ap.add_argument("--regenerate", action="store_true", help="regenerate the sample source file")
    ap.add_argument("--query", help="run an ad-hoc SQL query on the mart (view v_sales is available) and exit")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")

    if a.query:
        db = sqlite3.connect(a.db); show("Ad-hoc query", q(db, a.query)); return
    os.makedirs(a.out, exist_ok=True)
    if a.regenerate or not os.path.exists(a.source): generate_source(a.source, a.rows)

    if os.path.exists(a.db): os.remove(a.db)
    db = sqlite3.connect(a.db); db.executescript(DDL)
    n_ext = extract(db, a.source)
    clean, rejects = transform(db)
    load(db, clean, rejects)
    db.execute("INSERT INTO etl_audit VALUES(?,?,?,?)", (datetime.datetime.now().isoformat(timespec="seconds"), n_ext, len(rejects), len(clean)))
    verify(db, n_ext, len(clean), len(rejects), sum(r["amount"] for r in clean))
    build_cube(db)
    db.commit()

    show("ETL REJECTS (data-quality report)", q(db, "SELECT txn_id, reason FROM etl_rejects ORDER BY CAST(txn_id AS INT)"), a.out, "etl_rejects.csv")
    show("CUBE cells: Year x Region (from materialised cube)", cube_lookup(db), a.out, "cube_year_region.csv")
    show("ROLL-UP: Quarter -> Year -> All", rollup(db), a.out, "olap_rollup.csv")
    show("DRILL-DOWN: 2025 Quarter -> Month", drilldown(db, 2025), a.out, "olap_drilldown_2025.csv")
    show("SLICE: category = Electronics", slice_(db, "Electronics"), a.out, "olap_slice_electronics.csv")
    show("DICE: 2025, West+South, Apparel+Electronics", dice(db, 2025, ["West", "South"], ["Apparel", "Electronics"]), a.out, "olap_dice.csv")
    show("PIVOT: Region x Category (2025)", pivot(db, 2025), a.out, "olap_pivot_2025.csv")
    show("TOP 3 PRODUCTS", top_products(db), a.out, "top_products.csv")
    show("YEAR-OVER-YEAR by region", yoy(db), a.out, "yoy_region.csv")
    show("CUBE apex + 1-D cuboids", q(db, "SELECT cuboid, year, region, category, segment, total_sales FROM cube_sales "
         "WHERE cuboid IN ('APEX','year','region','category','segment') ORDER BY cuboid, 2,3,4,5"), a.out, "cube_1d_cuboids.csv")
    print(f"\nDone. Database: {a.db}\nReports (CSV): {a.out}")

if __name__ == "__main__":
    main()
