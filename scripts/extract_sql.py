import json
import re

# Path to your Jupyter Notebook
notebook_path = "notebooks/Fetch.ipynb"
output_sql_file = "queries/extracted_queries.sql"

# Load the Jupyter Notebook
with open(notebook_path, "r", encoding="utf-8") as f:
    notebook = json.load(f)

# Extract SQL queries from code cells
sql_queries = []
sql_pattern = re.compile(r"(SELECT|INSERT|UPDATE|DELETE|WITH)\s+", re.IGNORECASE)

for cell in notebook["cells"]:
    if cell["cell_type"] == "code":
        source_code = "".join(cell["source"])
        if source_code.strip().lower().startswith("%%sql") or sql_pattern.search(source_code):
            sql_queries.append(source_code.replace("%%sql", "").strip())

# Save to a .sql file
if sql_queries:
    with open(output_sql_file, "w", encoding="utf-8") as sql_file:
        sql_file.write("\n\n".join(sql_queries))
    print(f"✅ SQL queries extracted and saved to {output_sql_file}")
else:
    print("⚠ No SQL queries found in the notebook.")

