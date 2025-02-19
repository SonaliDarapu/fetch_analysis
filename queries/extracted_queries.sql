

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
