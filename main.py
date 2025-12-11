import os
import sys
import subprocess
from typing import List, Dict, Optional

# --- Module-level Constants ---
DB_FILE = "recap_database.duckdb"
DBT_PROJECT_DIR = "dbt_recap"
DLT_PIPELINE_SCRIPT = "dlt/pipeline.py"
DUCKDB_SETUP_SCRIPT = "duckdb_setup/create_source_tables.py"


def run_command(command: List[str], step_name: str, env: Optional[Dict[str, str]] = None) -> None:
    """
    Executes a command-line process and ensures graceful failure handling.

    Args:
        command: A list of strings representing the command and its arguments.
        step_name: A user-friendly name for the step being executed.
        env: Optional environment variables to pass to the subprocess.
    """
    print(f"\n--- STARTING: {step_name} ---")

    try:
        if command[0].lower() == 'dbt':
            print("Note: If the dbt command fails, ensure your virtual environment (venv) is active.")

        result = subprocess.run(
            command,
            check=True,
            shell=False,
            capture_output=True,
            text=True,
            env=env
        )
        print(f"--- SUCCESS: {step_name} ---")
        print(result.stdout)

    except subprocess.CalledProcessError as error:
        print(f"--- FAILED: {step_name} ---")
        print(f"Command failed: {' '.join(error.cmd)}")
        print(f"Stdout:\n{error.stdout}")
        print(f"Stderr:\n{error.stderr}")
        sys.exit(1)

    except FileNotFoundError:
        print(f"--- FAILED: {step_name} ---")
        print(f"Error: Command executable ('{command[0]}') not found.")
        sys.exit(1)


def main():
    """Runs the full data pipeline sequentially: DLT -> DuckDB Setup -> DBT Build."""
    print("Starting full data pipeline orchestration...")

    # --- Step 0: Cleanup ---
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Cleaned up previous database file: {DB_FILE}")

    # --- Step 1: dlt pipeline (Data ingestion) ---
    dlt_command = [sys.executable, DLT_PIPELINE_SCRIPT]
    run_command(dlt_command, "dlt Data Ingestion (API to CSV)")

    # --- Step 2: DuckDB setup (Source table materialisation) ---
    duckdb_setup_command = [sys.executable, DUCKDB_SETUP_SCRIPT]
    run_command(duckdb_setup_command, "DuckDB Source Table Setup (CSV to DuckDB)")

    # --- Step 3: dbt build (transformations, tests, docs) ---

    # 3a. Prepare Environment Variables for dbt
    dbt_env = os.environ.copy()
    dbt_env['DBT_DATABASE'] = os.path.basename(DB_FILE).replace('.duckdb', '')

    # 3b. Define and run the dbt command
    dbt_command = ["dbt", "build", "--project-dir", DBT_PROJECT_DIR]
    run_command(dbt_command, "dbt Project build", env=dbt_env)
    
    print("\n\n--- FULL PIPELINE COMPLETED SUCCESSFULLY! ---")


if __name__ == "__main__":
    main()