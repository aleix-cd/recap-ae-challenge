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

print("Invoices table preview:")
con.sql("select * from invoices limit 5").show()

print("\nInvoice-Transaction Matches table preview:")
con.sql("select * from invoice_transaction_matches limit 5").show()

con.close()
