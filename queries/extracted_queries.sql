# Check the missing values in the Products DataFrame
print("\nMissing Values in Products DataFrame before handling:")
print(df_products.isnull().sum())

# Fill missing product categories using mode (most frequent value)
df_products["CATEGORY_1"].fillna(df_products["CATEGORY_1"].mode()[0], inplace=True)
df_products["CATEGORY_2"].fillna(df_products["CATEGORY_2"].mode()[0], inplace=True)

# Fill CATEGORY_3 and CATEGORY_4 with 'Unknown' due to high missing values
df_products["CATEGORY_3"].fillna("Unknown", inplace=True)
df_products["CATEGORY_4"].fillna("Unknown", inplace=True)

# Fill MANUFACTURER and BRAND with 'Unknown' as they have a high percentage of missing values
df_products["MANUFACTURER"].fillna("Unknown", inplace=True)
df_products["BRAND"].fillna("Unknown", inplace=True)

# Investigate missing BARCODE values
print("\nMissing BARCODE values in Products:", df_products["BARCODE"].isnull().sum())

# Convert BARCODE to string type and fill missing values with 'Unknown'
df_products["BARCODE"] = df_products["BARCODE"].astype(str).fillna("Unknown")

# Confirm missing values are handled
print("\nMissing Values in Products DataFrame after handling:")
print(df_products.isnull().sum())

# Convert date columns to datetime format
df_transactions["PURCHASE_DATE"] = pd.to_datetime(df_transactions["PURCHASE_DATE"], errors="coerce")
df_transactions["SCAN_DATE"] = pd.to_datetime(df_transactions["SCAN_DATE"], errors="coerce")

# Convert BARCODE to string for consistency with df_products
df_transactions["BARCODE"] = df_transactions["BARCODE"].astype(str)

# Convert FINAL_QUANTITY and FINAL_SALE to float
df_transactions["FINAL_QUANTITY"] = pd.to_numeric(df_transactions["FINAL_QUANTITY"], errors="coerce")
df_transactions["FINAL_SALE"] = pd.to_numeric(df_transactions["FINAL_SALE"], errors="coerce")

# Find transactions where BARCODE does not exist in Products
missing_barcodes = df_transactions[~df_transactions["BARCODE"].isin(df_products["BARCODE"])]

# Print count of missing BARCODEs
print(f"🚨 Number of transactions with missing BARCODEs: {len(missing_barcodes)}")

# Display a few rows of missing BARCODE transactions
display(missing_barcodes.head())

# Convert BARCODE to string format and remove leading/trailing spaces
df_products["BARCODE"] = df_products["BARCODE"].astype(str).str.strip()
df_transactions["BARCODE"] = df_transactions["BARCODE"].astype(str).str.strip()

# Check again if missing BARCODEs are reduced
missing_barcodes = df_transactions[~df_transactions["BARCODE"].isin(df_products["BARCODE"])]
print(f"🚨 Number of transactions with missing BARCODEs after formatting fix: {len(missing_barcodes)}")

# Find barcodes in transactions that do not exist in products
missing_barcodes = df_transactions[~df_transactions["BARCODE"].isin(df_products["BARCODE"])]
print("\nTransactions with Unmatched Barcodes:", len(missing_barcodes))

# Filling missing values in categorical columns with 'Unknown'
df_products["CATEGORY_4"] = df_products["CATEGORY_4"].fillna("Unknown")
df_products["MANUFACTURER"] = df_products["MANUFACTURER"].fillna("Unknown")
df_products["BRAND"] = df_products["BRAND"].fillna("Unknown")

df_users["GENDER"] = df_users["GENDER"].fillna("Unknown")
df_users["LANGUAGE"] = df_users["LANGUAGE"].fillna("Unknown")
df_users["STATE"] = df_users["STATE"].fillna("Unknown")

# Handling missing values in numerical fields
df_transactions["FINAL_SALE"] = df_transactions["FINAL_SALE"].replace(" ", np.nan).astype(float)
df_transactions["FINAL_QUANTITY"] = df_transactions["FINAL_QUANTITY"].replace("zero", "0").astype(float)

# Verify if missing values are handled
print("\nMissing Values After Cleaning:")
print(df_products.isnull().sum())
print(df_transactions.isnull().sum())
print(df_users.isnull().sum())

missing_barcodes = df_transactions[~df_transactions["BARCODE"].isin(df_products["BARCODE"])]
print(f"🚨 Transactions with missing BARCODEs: {len(missing_barcodes)}")

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite (assuming df_users and df_transactions exist)
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# SQL query using subqueries
query = """

    SELECT 
        t.BRAND,
        COUNT(DISTINCT t.RECEIPT_ID) AS receipt_count
    FROM df_transactions t
    JOIN df_users u ON t.USER_ID = u.USER_ID
    WHERE u.AGE >= 21
    GROUP BY t.BRAND
;
"""

# Execute the query and store results in a DataFrame
top_5_brands = pd.read_sql_query(query, conn)
print(top_5_brands)
# Close the database connection
conn.close()

# Display results
import ace_tools as tools
tools.display_dataframe_to_user(name="Top 5 Brands by Receipts Scanned (Users 21+)", dataframe=top_5_brands)

import sqlite3
import pandas as pd

# Ensure AGE is calculated from BIRTH_DATE
df_users["AGE"] = pd.to_datetime("today").year - pd.to_datetime(df_users["BIRTH_DATE"]).dt.year

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)
df_products.to_sql("df_products", conn, if_exists="replace", index=False)

# SQL query with AGE calculation
query = """
SELECT p.BRAND, receipt_count 
FROM (
    SELECT 
        t.BARCODE,  
        COUNT(DISTINCT t.RECEIPT_ID) AS receipt_count
    FROM df_transactions t
    JOIN (
        SELECT ID, 
               (strftime('%Y', 'now') - strftime('%Y', BIRTH_DATE)) AS AGE
        FROM df_users
    ) u ON t.USER_ID = u.ID
    WHERE u.AGE >= 21
    GROUP BY t.BARCODE
) sub
JOIN df_products p ON sub.BARCODE = p.BARCODE  
ORDER BY receipt_count DESC
LIMIT 5;
"""

# Execute the query
top_5_brands = pd.read_sql_query(query, conn)

# Close database connection
conn.close()

print(top_5_brands)

import sqlite3
import pandas as pd
from datetime import datetime

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Ensure CREATED_DATE is in datetime format before inserting into SQLite
df_users["CREATED_DATE"] = pd.to_datetime(df_users["CREATED_DATE"])

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)
df_products.to_sql("df_products", conn, if_exists="replace", index=False)

# SQL query using SQLite to find top 5 brands by sales among users with accounts at least 6 months old
query = """
WITH eligible_users AS (
    SELECT ID 
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
)
SELECT p.BRAND, SUM(t.FINAL_SALE) AS total_sales
FROM df_transactions t
JOIN eligible_users u ON t.USER_ID = u.ID
JOIN df_products p ON t.BARCODE = p.BARCODE
GROUP BY p.BRAND
ORDER BY total_sales DESC
LIMIT 6;
"""

# Execute the query
top_5_brands_sales = pd.read_sql_query(query, conn)
print(top_5_brands_sales)
# Close database connection
conn.close()

import sqlite3
import pandas as pd
from datetime import datetime

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Ensure CREATED_DATE is in datetime format before inserting into SQLite
df_users["CREATED_DATE"] = pd.to_datetime(df_users["CREATED_DATE"])

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# SQL query to identify Fetch power users (active 6+ months, 10+ receipts per month, $500+ spending per month)
query = """
WITH active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', t.PURCHASE_DATE) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(t.FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 5 AND total_spending >= 5
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""

# Execute the query and store results in a DataFrame
power_users = pd.read_sql_query(query, conn)

# Close database connection
conn.close()

print(power_users)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite (Assuming df_users and df_transactions exist)
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# Step 1: Check if there are users with accounts older than 6 months
query_check_users = """
SELECT COUNT(*) AS eligible_users 
FROM df_users 
WHERE CREATED_DATE <= DATE('now', '-6 months');
"""
eligible_users = pd.read_sql_query(query_check_users, conn)

# Step 2: Check transaction activity (receipts scanned per user)
query_check_receipts = """
SELECT USER_ID, COUNT(DISTINCT RECEIPT_ID) AS receipt_count
FROM df_transactions
GROUP BY USER_ID
ORDER BY receipt_count DESC
LIMIT 5;
"""
top_receipt_users = pd.read_sql_query(query_check_receipts, conn)

# Step 3: Check user spending activity per user
query_check_spending = """
SELECT USER_ID, SUM(FINAL_SALE) AS total_spending
FROM df_transactions
GROUP BY USER_ID
ORDER BY total_spending DESC
LIMIT 5;
"""
top_spending_users = pd.read_sql_query(query_check_spending, conn)

# Step 4: Check if transactions have valid purchase dates
query_check_purchase_dates = """
SELECT COUNT(*) AS missing_dates FROM df_transactions WHERE PURCHASE_DATE IS NULL;
"""
missing_dates = pd.read_sql_query(query_check_purchase_dates, conn)

# Close database connection
conn.close()
print(eligible_users)
print(top_receipt_users)
print(top_spending_users)
print(missing_dates)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# Updated SQL query with lower thresholds (5+ receipts and $200+ spending)
query = """
WITH active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', t.PURCHASE_DATE) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(t.FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 5 AND total_spending >= 200
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""

# Execute the query and store results in a DataFrame
power_users = pd.read_sql_query(query, conn)

# Close database connection
conn.close()

print(power_users)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# Step 3: Verify users with both 5+ receipts per month and $200+ spending per month
query_step3 = """
SELECT USER_ID, 
       strftime('%Y-%m', PURCHASE_DATE) AS purchase_month,
       COUNT(DISTINCT RECEIPT_ID) AS total_receipts,
       SUM(FINAL_SALE) AS total_spending
FROM df_transactions
GROUP BY USER_ID, purchase_month
HAVING total_receipts >= 5 AND total_spending >= 200
LIMIT 10;
"""
step3_results = pd.read_sql_query(query_step3, conn)

# Step 4: Check how many users have accounts older than 6 months
query_step4 = """
SELECT COUNT(*) AS eligible_users
FROM df_users 
WHERE CREATED_DATE <= DATE('now', '-6 months');
"""
step4_results = pd.read_sql_query(query_step4, conn)

# Step 5: Check final number of users meeting all conditions
query_step5 = """
WITH active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', t.PURCHASE_DATE) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(t.FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 5 AND total_spending >= 200
)
SELECT COUNT(*) AS final_eligible_users FROM monthly_activity;
"""
step5_results = pd.read_sql_query(query_step5, conn)

# Close database connection
conn.close()

print(step3_results)
print(step4_results)
print(step5_results)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# Step 1: Check if any users scan 5+ receipts per month
query_check_receipts = """
SELECT USER_ID, COUNT(DISTINCT RECEIPT_ID) AS total_receipts
FROM df_transactions
GROUP BY USER_ID
ORDER BY total_receipts DESC
LIMIT 10;
"""
top_receipt_users = pd.read_sql_query(query_check_receipts, conn)

# Step 2: Check the highest spenders per month
query_check_spending = """
SELECT USER_ID, strftime('%Y-%m', PURCHASE_DATE) AS purchase_month, SUM(FINAL_SALE) AS total_spending
FROM df_transactions
GROUP BY USER_ID, purchase_month
ORDER BY total_spending DESC
LIMIT 10;
"""
top_spending_users = pd.read_sql_query(query_check_spending, conn)

# Step 3: Adjusted query with lower criteria (3+ receipts, $100+ spending)
query_adjusted = """
WITH active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', t.PURCHASE_DATE) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(t.FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 1 AND total_spending > 10
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""
adjusted_results = pd.read_sql_query(query_adjusted, conn)

# Close database connection
conn.close()

print(adjusted_results)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# SQL Query: Identify power users in the top 10% spenders with 3+ receipts per month
query_top_spenders = """
WITH spending_percentiles AS (
    SELECT total_spent
    FROM (
        SELECT USER_ID, SUM(FINAL_SALE) AS total_spent
        FROM df_transactions
        GROUP BY USER_ID
    )
    ORDER BY total_spent DESC
    LIMIT (SELECT COUNT(*) * 0.10 FROM df_transactions)  -- Selects top 10% of spenders
),
active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', t.PURCHASE_DATE) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(t.FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 3 AND total_spending >= (SELECT MIN(total_spent) FROM spending_percentiles)
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""

# Execute the query and store results in a DataFrame
top_power_users = pd.read_sql_query(query_top_spenders, conn)

# Close database connection
conn.close()

print(top_power_users)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# Step 1: Check Column Datatypes Before Fixing
df_info = pd.read_sql_query("PRAGMA table_info(df_transactions);", conn)

# Step 2: Convert FINAL_SALE to FLOAT if needed
conn.execute("""
ALTER TABLE df_transactions ADD COLUMN FINAL_SALE_FLOAT FLOAT;
""")

conn.execute("""
UPDATE df_transactions 
SET FINAL_SALE_FLOAT = CAST(FINAL_SALE AS FLOAT) 
WHERE FINAL_SALE IS NOT NULL AND FINAL_SALE != '';
""")

conn.execute("""
ALTER TABLE df_transactions DROP COLUMN FINAL_SALE;
""")

conn.execute("""
ALTER TABLE df_transactions RENAME COLUMN FINAL_SALE_FLOAT TO FINAL_SALE;
""")

# Step 3: Convert PURCHASE_DATE to DATE if needed
conn.execute("""
ALTER TABLE df_transactions ADD COLUMN PURCHASE_DATE_DATE DATE;
""")

conn.execute("""
UPDATE df_transactions 
SET PURCHASE_DATE_DATE = DATE(PURCHASE_DATE) 
WHERE PURCHASE_DATE IS NOT NULL AND PURCHASE_DATE != '';
""")

conn.execute("""
ALTER TABLE df_transactions DROP COLUMN PURCHASE_DATE;
""")

conn.execute("""
ALTER TABLE df_transactions RENAME COLUMN PURCHASE_DATE_DATE TO PURCHASE_DATE;
""")

# Step 4: Execute SQL Query After Fixing Datatypes
query_top_spenders = """
WITH spending_percentiles AS (
    SELECT CAST(total_spent AS FLOAT) AS total_spent
    FROM (
        SELECT USER_ID, SUM(FINAL_SALE) AS total_spent
        FROM df_transactions
        GROUP BY USER_ID
    )
    ORDER BY total_spent DESC
    LIMIT (SELECT COUNT(DISTINCT USER_ID) * 0.10 FROM df_transactions)  
),
active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE DATE(CREATED_DATE) <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', DATE(t.PURCHASE_DATE)) AS purchase_month,
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 3 
    AND total_spending >= (SELECT MIN(total_spent) FROM spending_percentiles)
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""

# Execute the query and store results in a DataFrame
top_power_users = pd.read_sql_query(query_top_spenders, conn)

# Close database connection
conn.close()

# Display results
import ace_tools as tools
tools.display_dataframe_to_user(name="Top 10% Spending Power Users", dataframe=top_power_users)

# Display updated table schema
tools.display_dataframe_to_user(name="Updated Table Schema", dataframe=df_info)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Step 1: Load Data into SQLite (Ensure the Data Exists)
try:
    df_users.to_sql("df_users", conn, if_exists="replace", index=False)
    df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)
except NameError:
    print("Datasets not found. Please upload df_users and df_transactions again.")

# Step 2: Check Column Datatypes in df_transactions
query_check_transactions_columns = "PRAGMA table_info(df_transactions);"
df_transactions_info = pd.read_sql_query(query_check_transactions_columns, conn)
print("Column Information for df_transactions:")
print(df_transactions_info)

# Step 3: Check Column Datatypes in df_users
query_check_users_columns = "PRAGMA table_info(df_users);"
df_users_info = pd.read_sql_query(query_check_users_columns, conn)
print("Column Information for df_users:")
print(df_users_info)

# Step 4: Ensure FINAL_SALE is stored as FLOAT
conn.execute("""
UPDATE df_transactions 
SET FINAL_SALE = CAST(FINAL_SALE AS FLOAT) 
WHERE FINAL_SALE IS NOT NULL AND FINAL_SALE != '';
""")

# Step 5: Ensure PURCHASE_DATE is stored as DATE
conn.execute("""
UPDATE df_transactions 
SET PURCHASE_DATE = DATE(PURCHASE_DATE) 
WHERE PURCHASE_DATE IS NOT NULL AND PURCHASE_DATE != '';
""")

# Step 6: Ensure CREATED_DATE is stored as DATE
conn.execute("""
UPDATE df_users 
SET CREATED_DATE = DATE(CREATED_DATE) 
WHERE CREATED_DATE IS NOT NULL AND CREATED_DATE != '';
""")

# Step 7: Check if Datatype Fixes Were Applied
df_transactions_info_after = pd.read_sql_query(query_check_transactions_columns, conn)
df_users_info_after = pd.read_sql_query(query_check_users_columns, conn)
print("Updated Column Information for df_transactions:")
print(df_transactions_info_after)
print("Updated Column Information for df_users:")
print(df_users_info_after)

# Step 8: Re-run the SQL Query
query_top_spenders = """
WITH spending_percentiles AS (
    SELECT total_spent
    FROM (
        SELECT USER_ID, SUM(FINAL_SALE) AS total_spent
        FROM df_transactions
        GROUP BY USER_ID
    )
    ORDER BY total_spent DESC
    LIMIT (SELECT COUNT(DISTINCT USER_ID) * 0.10 FROM df_transactions)  
),
active_users AS (
    SELECT ID AS USER_ID
    FROM df_users
    WHERE CREATED_DATE <= DATE('now', '-6 months')
),
monthly_activity AS (
    SELECT 
        t.USER_ID, 
        strftime('%Y-%m', PURCHASE_DATE) AS purchase_month,  
        COUNT(DISTINCT t.RECEIPT_ID) AS total_receipts, 
        SUM(FINAL_SALE) AS total_spending
    FROM df_transactions t
    JOIN active_users u ON t.USER_ID = u.USER_ID
    GROUP BY t.USER_ID, purchase_month
    HAVING total_receipts >= 3 
    AND total_spending >= (SELECT MIN(total_spent) FROM spending_percentiles)
)
SELECT USER_ID, COUNT(DISTINCT purchase_month) AS active_months, SUM(total_spending) AS total_spent
FROM monthly_activity
GROUP BY USER_ID
ORDER BY total_spent DESC;
"""

# Step 9: Execute the Query and Handle Errors
try:
    top_power_users = pd.read_sql_query(query_top_spenders, conn)
    # Close database connection
    conn.close()

    # Display results
    import ace_tools as tools
    tools.display_dataframe_to_user(name="Top 10% Spending Power Users", dataframe=top_power_users)

except Exception as e:
    print(f"Error executing query: {e}")
    conn.close()

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load the user's dataset into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)
df_products.to_sql("df_products", conn, if_exists="replace", index=False)

# SQL Query to find the leading brand in the "Dips & Salsa" category by total sales
query_leading_brand = """
SELECT p.brand, SUM(t.sale) AS total_sales
FROM transactions t
JOIN products p ON t.barcode = p.barcode
WHERE 'Dips & Salsa' IN (p.category_1, p.category_2, p.category_3, p.category_4)
GROUP BY p.brand
ORDER BY total_sales DESC
LIMIT 1;
"""

# Execute the query and store results in a DataFrame
leading_brand = pd.read_sql_query(query_leading_brand, conn)

# Close database connection
conn.close()

# Display results
import ace_tools as tools
tools.display_dataframe_to_user(name="Leading Brand in Dips & Salsa Category", dataframe=leading_brand)

# Ensure SQLite database is active
conn = sqlite3.connect(":memory:")

# Reload data into SQLite
df_transactions.to_sql("transactions", conn, if_exists="replace", index=False)
df_products.to_sql("products", conn, if_exists="replace", index=False)

# Confirm the tables are now available
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("Tables now available in SQLite:")
print(tables)

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Reload the data into SQLite if it exists in memory
df_transactions.to_sql("transactions", conn, if_exists="replace", index=False)
df_products.to_sql("products", conn, if_exists="replace", index=False)

# Step 1: Verify if the tables exist in SQLite
query_check_tables = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql_query(query_check_tables, conn)
print("Tables in SQLite Database:")
print(tables)

# Step 2: Check column names in transactions
query_check_transactions_columns = "PRAGMA table_info(transactions);"
df_transactions_info = pd.read_sql_query(query_check_transactions_columns, conn)
print("Column Information for transactions:")
print(df_transactions_info)

# Step 3: Check column names in products
query_check_products_columns = "PRAGMA table_info(products);"
df_products_info = pd.read_sql_query(query_check_products_columns, conn)
print("Column Information for products:")
print(df_products_info)

# Step 4: Re-run the SQL Query to find the leading brand in "Dips & Salsa"

query_leading_brand = """
SELECT p.BRAND, SUM(t.FINAL_SALE) AS total_sales
FROM transactions t
JOIN products p ON t.BARCODE = p.BARCODE
WHERE 'Dips & Salsa' IN (p.CATEGORY_1, p.CATEGORY_2, p.CATEGORY_3, p.CATEGORY_4)
GROUP BY p.BRAND
ORDER BY total_sales DESC
LIMIT 2;
"""


# Execute the query and store results in a DataFrame
try:
    leading_brand = pd.read_sql_query(query_leading_brand, conn)

    print(leading_brand)
except Exception as e:
    print(f"Error executing query: {e}")

# Close database connection
conn.close()

import sqlite3
import pandas as pd

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("df_transactions", conn, if_exists="replace", index=False)

# SQL Query 1: Year-over-Year Growth Based on User Registrations
query_user_growth = """
WITH user_counts AS (
    SELECT strftime('%Y', CREATED_DATE) AS year, COUNT(*) AS user_count
    FROM df_users
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        user_count,
        LAG(user_count) OVER (ORDER BY year) AS previous_year_count,
        CASE 
            WHEN LAG(user_count) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((user_count - LAG(user_count) OVER (ORDER BY year)) * 100.0 / LAG(user_count) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM user_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# SQL Query 2: Year-over-Year Growth Based on Total Receipts Scanned
query_transaction_growth = """
WITH transaction_counts AS (
    SELECT strftime('%Y', PURCHASE_DATE) AS year, COUNT(*) AS receipt_count
    FROM transactions
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        receipt_count,
        LAG(receipt_count) OVER (ORDER BY year) AS previous_year_count,
        CASE 
            WHEN LAG(receipt_count) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((receipt_count - LAG(receipt_count) OVER (ORDER BY year)) * 100.0 / LAG(receipt_count) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM transaction_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# SQL Query 3: Year-over-Year Growth Based on Total Sales (FINAL_SALE)
query_sales_growth = """
WITH sales_counts AS (
    SELECT strftime('%Y', PURCHASE_DATE) AS year, SUM(FINAL_SALE) AS total_sales
    FROM transactions
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        total_sales,
        LAG(total_sales) OVER (ORDER BY year) AS previous_year_sales,
        CASE 
            WHEN LAG(total_sales) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((total_sales - LAG(total_sales) OVER (ORDER BY year)) * 100.0 / LAG(total_sales) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM sales_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# Execute the queries and store results in DataFrames
user_growth = pd.read_sql_query(query_user_growth, conn)
transaction_growth = pd.read_sql_query(query_transaction_growth, conn)
sales_growth = pd.read_sql_query(query_sales_growth, conn)

# Close database connection
conn.close()
print("Year-over-Year User Growth", user_growth)
# Display results
import ace_tools as tools
tools.display_dataframe_to_user(name="Year-over-Year User Growth", dataframe=user_growth)
tools.display_dataframe_to_user(name="Year-over-Year Transaction Growth", dataframe=transaction_growth)
tools.display_dataframe_to_user(name="Year-over-Year Sales Growth", dataframe=sales_growth)

import sqlite3

# Create a new in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("transactions", conn, if_exists="replace", index=False)

# Confirm that the tables exist
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("Tables now available in SQLite:")
print(tables)

# Confirm that the tables exist
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("Tables now available in SQLite:")
print(tables)

import sqlite3

# Create a new in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load data into SQLite
df_users.to_sql("df_users", conn, if_exists="replace", index=False)
df_transactions.to_sql("transactions", conn, if_exists="replace", index=False)
# SQL Query 1: Year-over-Year Growth Based on User Registrations
query_user_growth = """
WITH user_counts AS (
    SELECT strftime('%Y', CREATED_DATE) AS year, COUNT(*) AS user_count
    FROM df_users
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        user_count,
        LAG(user_count) OVER (ORDER BY year) AS previous_year_count,
        CASE 
            WHEN LAG(user_count) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((user_count - LAG(user_count) OVER (ORDER BY year)) * 100.0 / LAG(user_count) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM user_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# SQL Query 2: Year-over-Year Growth Based on Total Receipts Scanned
query_transaction_growth = """
WITH transaction_counts AS (
    SELECT strftime('%Y', PURCHASE_DATE) AS year, COUNT(*) AS receipt_count
    FROM transactions
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        receipt_count,
        LAG(receipt_count) OVER (ORDER BY year) AS previous_year_count,
        CASE 
            WHEN LAG(receipt_count) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((receipt_count - LAG(receipt_count) OVER (ORDER BY year)) * 100.0 / LAG(receipt_count) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM transaction_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# SQL Query 3: Year-over-Year Growth Based on Total Sales (FINAL_SALE)
query_sales_growth = """
WITH sales_counts AS (
    SELECT strftime('%Y', PURCHASE_DATE) AS year, SUM(FINAL_SALE) AS total_sales
    FROM transactions
    GROUP BY year
),
growth AS (
    SELECT 
        year,
        total_sales,
        LAG(total_sales) OVER (ORDER BY year) AS previous_year_sales,
        CASE 
            WHEN LAG(total_sales) OVER (ORDER BY year) IS NOT NULL 
            THEN ROUND(((total_sales - LAG(total_sales) OVER (ORDER BY year)) * 100.0 / LAG(total_sales) OVER (ORDER BY year)), 2)
            ELSE NULL
        END AS yoy_growth_percent
    FROM sales_counts
)
SELECT * FROM growth ORDER BY year DESC;
"""

# Execute the queries and store results in DataFrames
user_growth = pd.read_sql_query(query_user_growth, conn)
transaction_growth = pd.read_sql_query(query_transaction_growth, conn)
sales_growth = pd.read_sql_query(query_sales_growth, conn)
print(user_growth)
print(transaction_growth)
print(sales_growth)
# Close database connection
conn.close()