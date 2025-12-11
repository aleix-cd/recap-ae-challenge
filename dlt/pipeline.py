import os
import logging
import csv
import sys
from typing import Iterable, Any, Dict, List, Optional

from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("invoices_to_single_csv")

# --- Constants for Configuration (Read from ENV) ---
DEFAULT_API_BASE = "https://europe-west3-recap-dev-347108.cloudfunctions.net/analytics-challenge-api"
DEFAULT_INVOICES_PATH = "/invoices"
DEFAULT_OUT_CSV_PATH = "./dlt/data/invoices_full.csv"


def extract_invoices_from_page(page: Any) -> Iterable[Dict]:
    """
    Normalise common response shapes into an iterable of invoice dicts.

    Supports:
      - list (page is a list of invoice objects)
      - dict with keys like 'invoices' / 'data' / 'items' / 'results'
      - single invoice dict (fallback if it looks like an invoice)
    """
    if page is None:
        return []
    if isinstance(page, list):
        return page
    if isinstance(page, dict):
        for key in ("invoices", "data", "items", "results"):
            value = page.get(key)
            if isinstance(value, list):
                return value
        # Fallback: if dict looks like a single invoice (e.g., from a single-item endpoint)
        if any(key in page for key in ("invoice_id", "invoiceId", "id", "invoice_number")):
            return [page]
    return []


def make_client(base_url: str, page_param: str = "page", base_page: int = 1) -> RESTClient:
    """
    Create RESTClient with PageNumberPaginator configured to read the total pages
    from the `total_pages` field in the API response.
    """
    paginator = PageNumberPaginator(
        base_page=base_page,
        page_param=page_param,
        total_path="total_pages",
        stop_after_empty_page=True
    )

    client = RESTClient(
        base_url=base_url,
        paginator=paginator,
        # data_selector is intentionally None, relying on extract_invoices_from_page
        data_selector=None 
    )
    return client


def fetch_all_invoices(client: RESTClient, path: str) -> List[Dict]:
    """
    Use client.paginate(path) to iterate pages and return a flat list of invoice dicts.
    """
    all_rows: List[Dict] = []
    page_count = 0
    row_count = 0
    
    for page in client.paginate(path):
        page_count += 1
        invoices = list(extract_invoices_from_page(page))
        row_count += len(invoices)
        all_rows.extend(invoices)
        logger.info(
            "Fetched page %s: %s invoices (total %s)",
            page_count,
            len(invoices),
            row_count
        )
        
    logger.info(
        "Finished pagination: %s pages, %s total invoice rows",
        page_count,
        row_count
    )
    return all_rows


def write_single_csv(rows: List[Dict], out_path: str) -> None:
    """
    Write the list of dicts to a single CSV file.

    Fieldnames are the union of all keys observed across rows.
    Missing values are written as empty strings.
    """
    if not rows:
        logger.warning("No rows to write. Creating an empty CSV at %s", out_path)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        open(out_path, "w", encoding="utf-8").close()
        return

    fieldnames: List[str] = sorted({key for row in rows for key in row.keys()})
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        
        for row in rows:
            # Replace None values with empty strings for CSV compatibility
            sanitised_row = {k: v if v is not None else "" for k, v in row.items()}
            writer.writerow(sanitised_row)
            
    logger.info("Wrote %s rows to single CSV: %s", len(rows), out_path)


def run_pipeline():
    """Reads environment variables, executes the fetching, and writes the CSV."""
    
    # --- Environment Variable Setup ---
    api_base = os.getenv("INVOICES_API_BASE", DEFAULT_API_BASE)
    invoices_path = os.getenv("INVOICES_PATH", DEFAULT_INVOICES_PATH)
    out_csv = os.getenv("OUT_CSV_PATH", DEFAULT_OUT_CSV_PATH)
    start_page_env = os.getenv("START_PAGE")
    
    base_page: Optional[int] = None
    if start_page_env is not None:
        try:
            base_page = int(start_page_env)
        except ValueError:
            logger.error("START_PAGE environment variable is not a valid integer.")
            sys.exit(1)

    logger.info(
        "Using API_BASE=%s INVOICES_PATH=%s OUT_CSV=%s START_PAGE=%s",
        api_base,
        invoices_path,
        base_page,
        out_csv
    )
    
    # --- Execution ---
    client = make_client(
        base_url=api_base,
        page_param="page",
        base_page=base_page if base_page is not None else 1
    )

    invoices = fetch_all_invoices(client, invoices_path)

    write_single_csv(invoices, out_csv)

    logger.info("Done. Single CSV available at: %s", out_csv)


if __name__ == "__main__":
    run_pipeline()