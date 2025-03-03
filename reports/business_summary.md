Are there any data quality issues present?
Missing Values
Products Table:
CATEGORY_1 has 111 missing values.
CATEGORY_2 has 1,424 missing values.
CATEGORY_3 has 60,566 missing values.
CATEGORY_4 has 778,093 missing values (severe missing data).
MANUFACTURER and BRAND both have ~226,472 missing values (critical issue).
BARCODE has 4,025 missing values (could affect transaction mapping).
Transactions Table:
BARCODE has 5,762 missing values, meaning some transactions may not be linked to a product.
 Users Table:
BIRTH_DATE has 3,675 missing values.
STATE has 4,812 missing values.
LANGUAGE has 30,508 missing values (significant).
GENDER has 5,892 missing values.

DATA TYPES:
BARCODE is stored as float64 instead of string/object (likely due to missing values).
FINAL_QUANTITY and FINAL_SALE in df_transactions are objects (strings) instead of numeric.
PURCHASE_DATE and SCAN_DATE in df_transactions should be converted to datetime.
CREATED_DATE and BIRTH_DATE in df_users should be converted to datetime.

Duplicate Rows:

Duplicate Rows in Products DataFrame: 215
Duplicate Rows in Transactions DataFrame: 171
Duplicate Rows in Users DataFrame: 0

Inconsistent Data:
Gender inconsistencies: Possible values like "M", "Male", "F", "Female", "UNKNOWN" may exist.
Final Quantity & Sale columns in transactions: If values contain "N/A" or "null", they need cleaning.

Transactions with Unmatched Barcodes: 19408
If there are unmatched barcodes, transactions may reference non-existent products.
Some barcodes may be missing in df_products due to null values.

Number of transactions with missing BARCODEs: 19408 This indicates data inconsistency by checking whether every transaction references a valid product barcode

Key fixes:
Ensuring Everything is a String First (astype(str))
Checking if x is a Valid Number Before Converting (replace('.', '', 1).isdigit())
Handling Non-Numeric Cases with "Unknown"

Standardized text-based categorical values (GENDER, LANGUAGE)
Handled missing values (NaN) in numerical columns
Fixed floating-point precision issues
Sorted and limited unique values for better readability
Are there any fields that are challenging to understand?
FINAL_QUANTITY Column, Some values seem unrealistically small (e.g., 0.01, 0.04, 0.09)
Are these decimal values representing weight-based items (e.g., produce, liquids) or should they be whole numbers (e.g., item counts)?
Large outliers (276.76)—is this a bulk order or a data entry error?
Some products in transactions may not have a matching product entry in df_products.
If NaN values were replaced with "Unknown", should they instead be flagged for missing product mapping?
FINAL_SALE Column
Challenge:
Some values are extremely small (0.01, 0.03, 0.04)—is this pricing per unit, a discount, or an error?
If NaN values existed, were they missing sales values or cancelled transactions?
Ensure no negative values (which might indicate refunds).
                 

Closed-ended questions:
2. What are the top 5 brands by receipts scanned among users 21 and over?
*Top 5 Brands by Receipts Scanned (Users 21+):*
REPHRESH - 55 receipts
SLIM JIM - 55 receipts
CARR'S - 55 receipts
APOTHECARE ESSENTIALS - 55 receipts
BAUSCH + LOMB - 55 receipts


What are the top 5 brands by sales among users that have had their account for at least six months?
*Top 5 Brands by Total Sales (Users with Account 6+ Months):*
ANNIE'S HOMEGROWN GROCERY - $8,614.56
 BAREFOOT - $8,255.62
DOVE - $7,939.56
ORIBE - $7,537.74
SHEA MOISTURE - $7,178.80

Which is the leading brand in the Dips & Salsa category?
Tostitos- $ 359214.44
At what percent has Fetch grown year over year?
User Growth Assumptions
Fetch's user base grew rapidly from 2014 to 2021, peaking in 2017 (+820% YoY).
Since 2022, Fetch's user growth has declined, with a -42.31% drop in 2023.
Fetch should focus on retaining users and improving engagement (receipts scanned & purchases).

The user_count column represents total new user registrations per year.
Fetch tracks user signups based on the CREATED_DATE in df_users.
A higher user_count means more users joined Fetch that year.
A drop in user_count indicates a decline in new user acquisition.
Negative YoY growth values (e.g., -42.31% in 2023) mean fewer people signed up compared to the previous year, not that users left the platform.
The dataset does not track user churn (drop-off rate), so we assume users stay after registering.
The first year in the dataset (2014) is Fetch’s starting point.
Since there’s no previous-year data for 2014, we assume Fetch started tracking users that year.

3. 
Email to Product/Business Leader

Subject: Key Data Quality Issues & Trends – Fetch Investigation

Dear Sir,
I’ve analyzed Fetch’s transaction, product, and user data, and here are some key findings:

Data Quality Issues
Missing Values:
Severe gaps in product details: CATEGORY_4 (778K missing), MANUFACTURER & BRAND (~226K missing).
Transactions missing BARCODEs (5,762 records)—some purchases may not be linked to products.
User attributes missing (e.g., LANGUAGE: 30K, STATE: 4.8K, GENDER: 5.8K).

Data Type & Consistency Issues:
BARCODE stored as float (should be string, causing mismatches).
FINAL_QUANTITY & FINAL_SALE incorrectly stored as text—potentially affecting revenue calculations.
Gender inconsistencies (“M”, “Male”, “UNKNOWN”).

Duplicate & Unmatched Records:
19,408 transactions reference non-existent products (missing barcodes).
215 duplicate products, 171 duplicate transactions.

Interesting Trend
User Growth Slowdown: Fetch saw a -42.31% decline in new user registrations in 2023 compared to the previous year. While growth peaked in 2017 (+820% YoY), sustaining user engagement is now a challenge. A potential focus area could be improving retention through targeted offers and incentives.

Outstanding Questions & Next Steps

Product Data Issues:
Can we source missing CATEGORY_4, MANUFACTURER, and BRAND information?
Should missing values be flagged as "Unknown," or do they need validation from another dataset?

Sales & Transaction Data:
FINAL_QUANTITY values (e.g., 0.01, 276.76): Are these weight-based or data errors?
FINAL_SALE: Some values are unrealistically low (e.g., $0.01). Do we need validation rules to flag outliers?

Growth Strategy:
Should we analyze retention metrics to better understand why user growth has slowed?
Would marketing insights help determine if engagement (e.g., receipts scanned) has also declined?
I’d appreciate your input on the above. Let me know how we can best align data fixes with business priorities.

Thanks,
Sonali



