#!/usr/bin/env python3
"""
download_gutenberg_by_id.py

- Reads JSON (remote raw URL by default, or a local JSON file with --input).
- Extracts Gutenberg book IDs from `detailURLString` entries like:
    "detailURLString": "https://www.gutenberg.org/ebooks/598"
  -> id = 598
- Constructs ZIP URL: https://www.gutenberg.org/cache/epub/{id}/pg{id}-h.zip
- Downloads ZIP files (concurrent workers, retries, polite delay).
- Optionally unzips into per-book folders.

Example:
  python download_gutenberg_by_id.py --workers 2 --delay 0.5 --unzip
"""

import argparse
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    raise SystemExit("Install `requests` (pip install requests)")

try:
    from tqdm import tqdm
    TQDM = True
except Exception:
    TQDM = False

RAW_JSON_URL = "https://raw.githubusercontent.com/erdemaksakal/gb/main/gutenberg_cache.json"
ZIP_URL_TEMPLATE = "https://www.gutenberg.org/cache/epub/{id}/pg{id}-h.zip"

# regex to find detailURLString values and capture id if given raw JSON line or object value
DETAIL_URL_RE = re.compile(r"https?://(?:www\.)?gutenberg\.org/ebooks/(\d+)", re.IGNORECASE)

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def make_session(retries=5, backoff=0.5, user_agent=None):
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET", "HEAD"]))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    ua = user_agent or "gutenberg-id-downloader/1.0 (+https://github.com/erdemaksakal/gb)"
    session.headers.update({"User-Agent": ua, "Accept": "*/*"})
    return session


def load_json_from_url(session, url, timeout=30):
    logging.info("Fetching JSON from %s", url)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def load_json_from_file(path):
    logging.info("Loading JSON from file %s", path)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_ids_in_obj(obj):
    """Recursively find Gutenberg IDs from detailURLString-like values in JSON-decoded object."""
    ids = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            # If the key is detailURLString (case-sensitive) handle it directly
            if k == "detailURLString" and isinstance(v, str):
                m = DETAIL_URL_RE.search(v)
                if m:
                    ids.add(m.group(1))
            else:
                ids.update(find_ids_in_obj(v))
    elif isinstance(obj, list):
        for item in obj:
            ids.update(find_ids_in_obj(item))
    elif isinstance(obj, str):
        # fallback: check any string
        m = DETAIL_URL_RE.search(obj)
        if m:
            ids.add(m.group(1))
    return ids


def zip_url_for_id(book_id):
    return ZIP_URL_TEMPLATE.format(id=book_id)


def safe_filename_from_url(url):
    parsed = urlparse(url)
    name = os.path.basename(parsed.path) or parsed.netloc + parsed.path.replace("/", "_")
    # minimal sanitization
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return name


def download_zip(session, url, out_dir, timeout=60, chunk_size=8192, delay_after=0.0):
    """Download a zip URL to out_dir. Returns (url, path, status)."""
    filename = safe_filename_from_url(url)
    out_path = os.path.join(out_dir, filename)
    tmp_path = out_path + ".part"

    if os.path.exists(out_path):
        return (url, out_path, "exists")

    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            # If server returns 404 or similar, raise so we can mark as missing
            r.raise_for_status()
            total = r.headers.get("Content-Length")
            total = int(total) if total and total.isdigit() else None
            with open(tmp_path, "wb") as f:
                if TQDM and total:
                    pbar = tqdm(total=total, unit="B", unit_scale=True, desc=filename, leave=False)
                else:
                    pbar = None
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        if pbar:
                            pbar.update(len(chunk))
                if pbar:
                    pbar.close()
        os.replace(tmp_path, out_path)
        if delay_after:
            time.sleep(delay_after)
        return (url, out_path, "ok")
    except requests.HTTPError as he:
        # cleanup partial
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return (url, None, f"http_error: {he.response.status_code}")
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return (url, None, f"error: {e}")


def unzip_to_folder(zip_path, target_dir):
    import zipfile
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(target_dir)
        return True, None
    except Exception as e:
        return False, str(e)


def main():
    p = argparse.ArgumentParser(description="Download Gutenberg ZIPs by extracting IDs from detailURLString entries.")
    p.add_argument("--input", "-i", help="Local JSON file to read. If omitted uses remote raw JSON.")
    p.add_argument("--output", "-o", default="gutenberg_zips", help="Directory to save ZIP files.")
    p.add_argument("--workers", "-w", type=int, default=2, help="Concurrent download workers (be polite).")
    p.add_argument("--delay", "-d", type=float, default=0.5, help="Delay (s) after each download in a worker.")
    p.add_argument("--unzip", action="store_true", help="Unzip downloaded files into per-book folders.")
    p.add_argument("--user-agent", default=None, help="Custom User-Agent header.")
    args = p.parse_args()

    os.makedirs(args.output, exist_ok=True)
    session = make_session(user_agent=args.user_agent)

    # Load JSON
    try:
        if args.input:
            data = load_json_from_file(args.input)
        else:
            data = load_json_from_url(session, RAW_JSON_URL)
    except Exception as e:
        logging.error("Failed to load JSON: %s", e)
        return

    ids = sorted(find_ids_in_obj(data))
    if not ids:
        logging.info("No Gutenberg IDs found.")
        return

    logging.info("Found %d Gutenberg IDs. Starting downloads...", len(ids))

    # Build zip urls for each id
    zip_urls = [(book_id, zip_url_for_id(book_id)) for book_id in ids]

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {ex.submit(download_zip, session, url, args.output, 60, 8192, args.delay): (book_id, url)
                   for book_id, url in zip_urls}
        for fut in as_completed(futures):
            book_id, url = futures[fut]
            try:
                url, path, status = fut.result()
            except Exception as e:
                url, path, status = url, None, f"error: {e}"
            results.append((book_id, url, path, status))
            if status == "ok":
                logging.info("[ok] id=%s -> %s", book_id, path)
                if args.unzip and path and path.lower().endswith(".zip"):
                    target = os.path.join(args.output, f"book_{book_id}")
                    os.makedirs(target, exist_ok=True)
                    ok, err = unzip_to_folder(path, target)
                    if ok:
                        logging.info("Unzipped id=%s -> %s", book_id, target)
                    else:
                        logging.warning("Unzip failed id=%s: %s", book_id, err)
            elif status == "exists":
                logging.info("[exists] id=%s -> %s", book_id, path)
            else:
                logging.warning("[%s] id=%s -> %s", status, book_id, url)

    # summary
    ok = sum(1 for r in results if r[3] == "ok")
    skipped = sum(1 for r in results if r[3] == "exists")
    failed = [r for r in results if not (r[3] in ("ok", "exists"))]
    logging.info("Done. downloaded=%d skipped=%d failed=%d", ok, skipped, len(failed))
    if failed:
        logging.info("Failures (id, url, status):")
        for book_id, url, path, status in failed:
            logging.info(" - %s  %s  %s", book_id, url, status)


if __name__ == "__main__":
    main()
