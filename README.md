# re:cap Analytics Engineering Challenge

This project contains a complete data pipeline that:
1. ingests invoices data using `dlt`,
2. materialises to tables the relevant data into a DuckDB instance,
2. transforms and tests things using `dbt`, and
3. performs the required analyses into dbt `marts`.

The entire pipeline is orchestrated through a single command: `main.py`.

## ⚙️ Prerequisites

This guide assumes you are running **macOS** and have the following tools installed:

* **Homebrew:** for package management (`brew`).
* **pyenv:** for managing Python versions.

## 1. Setup environment and dependencies

We will use `pyenv` to make sure the we're all using the smae Python version, and `venv` for package management.

### 1.1. Install Python version

Ensure the required Python version is installed and set locally:

```bash
pyenv install 3.11.11
pyenv local 3.11.11
```

### 1.2. Create and activate the virtual environment

Create and activate the environment (venv):

```bash
python3 -m venv venv
source venv/bin/activate
```

### 1.3. Install Python dependencies

You will have a `requirements.txt` file listing all the dependencies, so just install from there:

```bash
python3 -m pip install -r requirements.txt
```

### 1.4. Install DuckDB via `homebrew`

Since you might want to run DuckDB from the command line (e.g. to run in-line queries), you will need to install
DuckDB via `homebrew` for an easy way to enable its CLI:

```bash
brew update
brew install duckdb
```

### 1.5. Create an `.env` file and source the variables

Create a file named `.env` in the root of the project with the following content:

```
DBT_DATABASE=~/path/to/your/recap_database.duckdb
```

And then, load the variable:

```bash
source .env
```

### 1.6. Configure dbt profiles

The project is configured to use the `recap_database.duckdb` file created in the root directory.

If your `profiles.yml` is not set up, you may need to ensure your profile points to the local DuckDB file.

Example of how your profile should look like:

```yml
dbt_recap:
  outputs:
    dev:
      type: duckdb
      path: '~/path/to/your/recap_database.duckdb'
      threads: 4
  target: dev
```

## 2. Run the full pipeline

The `main.py` script orchestrates the entire workflow sequentially:
1. **Ingestion**: Runs `dlt/pipeline.py` to fetch API data and save it to `dlt/data/invoices_full.csv`.
2. **Source setup**: Runs `duckdb_setup/create_source_tables.py` to load the CSV data into the `recap_database.duckdb` file.
3. **Transformation & analysis**: Runs dbt build on the `dbt_recap/` project, which executes all models, runs tests, and enables analysis tables.

Ensure your Python `venv` is active, and execute the script from the project root:

```bash
python3 main.py
```

## 3. Review analyses results

After a successful run, the proposed analyses will be materialised as tables.

You can run and see the results directly using the DuckDB CLI. See an example below:

```bash
duckdb recap_database.duckdb

select * from main.top_5_customer_reconciliation;

.exit
```

## Notes for reviewers:

1. I created a synthetic FX Exchange Rates dataset, which you will find in `dbt_recap/seeds/ecb_exchange_rates.csv`. The purpose of this was to be able to convert to a single currency in the `monetary_amount_active_rejected_matches.sql` model.
2. I was hitting a time constraint, so I did not write any documentation nor testing for the models in `dbt_recap/models/marts/core`.
3. I also simulated as if the `invoice_transaction_matches.csv` file was fetched from `dlt`, and so I just put it in the `dlt/data/` directory. I could've also treat it as a `dbt seed` instead.