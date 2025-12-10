import os
import logging
from typing import Iterable, Any, Dict, List

from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("invoices_to_single_csv")


def extract_invoices_from_page(page: Any) -> Iterable[Dict]:
    """
    Normalize common response shapes into an iterable of invoice dicts.
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
        for k in ("invoices", "data", "items", "results"):
            v = page.get(k)
            if isinstance(v, list):
                return v
        # fallback: if dict looks like a single invoice
        if any(k in page for k in ("invoice_id", "invoiceId", "id", "invoice_number")):
            return [page]
    return []


def make_client(base_url: str, page_param: str = "page", base_page: int = 1) -> RESTClient:
    """
    Create RESTClient with PageNumberPaginator configured to read the total pages
    from the `total_pages` field in the API response. This matches the API response
    you posted which includes 'total_pages' and 'total_items'.

    If your API ever uses a different key (e.g. 'total' or 'totalItems'), change
    total_path accordingly (it's a JSONPath expression).
    """
    # PageNumberPaginator signature: PageNumberPaginator(base_page=0, page=None,
    #    page_param=None, total_path="total", maximum_page=None, stop_after_empty_page=True, *, has_more_path=None, page_body_path=None)
    paginator = PageNumberPaginator(
        base_page=base_page,
        page_param=page_param,
        total_path="total_pages",
        stop_after_empty_page=True
    )

    client = RESTClient(
        base_url=base_url,
        paginator=paginator,
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
        log.info("Fetched page %s: %s invoices (total %s)", page_count, len(invoices), row_count)
    log.info("Finished pagination: %s pages, %s total invoice rows", page_count, row_count)
    return all_rows


def write_single_csv(rows: List[Dict], out_path: str) -> None:
    """
    Write the list of dicts to a single CSV file.
    Fieldnames are the union of all keys observed across rows.
    Missing values are written as empty strings.
    """
    if not rows:
        log.warning("No rows to write. Creating an empty CSV at %s", out_path)
        # create empty file
        open(out_path, "w", encoding="utf-8").close()
        return

    # compute union of all keys in stable order (sorted for reproducibility)
    fieldnames: List[str] = sorted({k for r in rows for k in r.keys()})
    import csv

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            # ensure we only write serializable primitives; convert None -> ""
            sanitized = {k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames}
            writer.writerow(sanitized)
    log.info("Wrote %s rows to single CSV: %s", len(rows), out_path)


if __name__ == "__main__":
    # configuration via env
    API_BASE = os.getenv(
        "INVOICES_API_BASE",
        "https://europe-west3-recap-dev-347108.cloudfunctions.net/analytics-challenge-api",
    )
    INVOICES_PATH = os.getenv("INVOICES_PATH", "/invoices")
    OUT_CSV = os.getenv("OUT_CSV_PATH", "./invoices_full.csv")
    START_PAGE = os.getenv("START_PAGE")

    base_page = int(START_PAGE) if START_PAGE is not None else 1

    log.info("Using API_BASE=%s INVOICES_PATH=%s OUT_CSV=%s START_PAGE=%s", API_BASE, INVOICES_PATH, OUT_CSV, base_page)

    client = make_client(base_url=API_BASE, page_param="page", base_page=base_page)

    # fetch all invoices (uses dlt's native paginator)
    invoices = fetch_all_invoices(client, INVOICES_PATH)

    # write to single CSV
    write_single_csv(invoices, OUT_CSV)

    log.info("Done. Single CSV available at: %s", OUT_CSV)