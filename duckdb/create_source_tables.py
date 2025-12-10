import duckdb

DB_FILE = "recap_database.duckdb"

INVOICES_CSV = "dlt/data/invoices_full.csv"
INVOICE_TRANSACTION_MATCHES_CSV = "dlt/data/invoice_transaction_matches.csv"

con = duckdb.connect(DB_FILE)

con.execute(f"""
    create or replace table invoices as
    select * from read_csv_auto('{INVOICES_CSV}');
""")

con.execute(f"""
    create or replace table invoice_transaction_matches as
    select * from read_csv_auto('{INVOICE_TRANSACTION_MATCHES_CSV}');
""")

# Optional: preview tables
print("Invoices preview:")
con.sql("SELECT * FROM invoices LIMIT 5").show()

print("\nInvoice-Transaction Matches preview:")
con.sql("SELECT * FROM invoice_transaction_matches LIMIT 5").show()

# Close connection
con.close()
