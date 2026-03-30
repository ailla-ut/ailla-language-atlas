#!/usr/bin/env python3
"""
AILLA Language Atlas Data Scraper
==================================

Systematically extracts metadata from the Archive of the Indigenous Languages
of Latin America (AILLA) through their public JSON API endpoints.

This scraper collects:
- Language metadata (639 languages)
- Collection metadata (259 collections)
- Country reference data (63 countries)
- Language family classifications
- Item-level metadata with temporal data (~19,785 items)

Output formats: CSV and structured JSON

Usage:
    uv run scripts/ailla_scraper.py                  # Full extraction (~52 min)
    uv run scripts/ailla_scraper.py --skip-items      # Quick mode (~2 min)
    uv run scripts/ailla_scraper.py --resume-items     # Resume interrupted items extraction
    uv run scripts/ailla_scraper.py --compare-ailla2   # Compare API items vs AILLA2 spreadsheets

Author: LBDS Fellow, Benson Latin American Collection
Date: 2026-01-28
Updated: 2026-02-22 (added items extraction with temporal data)
"""

import requests
import pandas as pd
import json
import time
import os
import argparse
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import sys

# Configuration
BASE_URL = "https://ailla-backend-prod.gsc1-pub.lib.utexas.edu"
HEADERS = {
    "User-Agent": "AILLA-Language-Atlas-Scraper/2.0 (LBDS Fellowship Project; Benson Collection)"
}
RATE_LIMIT_DELAY = 1.5  # seconds between requests
REQUEST_TIMEOUT = 60  # seconds (increased from 30 for slow API responses)
ITEMS_PER_PAGE = 10  # items endpoint hard-caps at 10 per page
CHECKPOINT_INTERVAL = 100  # save checkpoint every N pages
CHECKPOINT_FILE = "data/ailla_items_checkpoint.json"


class AILLAScraper:
    """Web scraper for AILLA metadata extraction."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.raw_data = {
            "languages": [],
            "collections": [],
            "countries": [],
            "items": [],
            "persons": [],
            "organizations": []
        }

    def fetch_paginated_endpoint(self, endpoint: str, entity_name: str) -> List[Dict[str, Any]]:
        """
        Fetch all pages from a paginated API endpoint.

        Args:
            endpoint: API endpoint path (e.g., '/languages')
            entity_name: Human-readable name for progress reporting

        Returns:
            List of all results from all pages
        """
        all_results = []
        page = 1

        print(f"\nFetching {entity_name}...")

        while True:
            url = f"{BASE_URL}{endpoint}?page={page}&per_page=15"

            try:
                print(f"  Page {page}...", end=" ", flush=True)
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()

                data = response.json()
                results = data.get("results", [])

                if not results:
                    print("(empty, stopping)")
                    break

                all_results.extend(results)
                print(f"({len(results)} records)")

                # Check if this is the last page
                total_pages = data.get("total_pages", 0)
                if page >= total_pages:
                    break

                page += 1
                time.sleep(RATE_LIMIT_DELAY)

            except requests.exceptions.RequestException as e:
                print(f"\n  ERROR on page {page}: {e}")
                print(f"  Continuing with {len(all_results)} records collected so far...")
                break

        print(f"  Total {entity_name} collected: {len(all_results)}")
        return all_results

    def fetch_items_endpoint(self, resume: bool = False) -> List[Dict[str, Any]]:
        """
        Fetch all items from the /items endpoint.

        The items endpoint has a hard cap of 10 results per page (distinct from
        the 15-per-page on other endpoints), resulting in ~1,979 pages for
        ~19,785 items. This method includes:
        - Progress reporting with ETA
        - Checkpoint saving every 100 pages for resume capability
        - Resume from checkpoint after interruption

        Args:
            resume: If True, attempt to resume from checkpoint file

        Returns:
            List of all item results from all pages
        """
        all_results = []
        start_page = 1

        # Resume from checkpoint if requested
        resumed_skipped: list[int] = []
        if resume and os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                all_results = checkpoint.get("results", [])
                start_page = checkpoint.get("next_page", 1)
                resumed_skipped = checkpoint.get("skipped_pages", [])
                print(f"\nResuming items extraction from checkpoint:")
                print(f"  Records recovered: {len(all_results)}")
                print(f"  Resuming from page: {start_page}")
                if resumed_skipped:
                    print(f"  Previously skipped pages: {len(resumed_skipped)}")
            except (json.JSONDecodeError, KeyError) as e:
                print(f"\nCheckpoint file corrupted, starting fresh: {e}")
                all_results = []
                start_page = 1
        elif resume:
            print("\nNo checkpoint file found, starting fresh extraction.")

        print(f"\nFetching items (10 per page, ~1,979 pages expected)...")
        print(f"  Estimated time: ~{int((1979 - start_page + 1) * RATE_LIMIT_DELAY / 60)} minutes remaining")

        page = start_page
        extraction_start = time.time()
        pages_fetched = 0
        total_pages = None
        skipped_pages: list[int] = list(resumed_skipped)
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 10

        while True:
            url = f"{BASE_URL}/items?page={page}&per_page={ITEMS_PER_PAGE}"

            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()

                data = response.json()
                results = data.get("results", [])

                if not results:
                    print(f"\n  Page {page}: empty, stopping")
                    break

                all_results.extend(results)
                pages_fetched += 1
                consecutive_failures = 0

                # Get total pages on first request
                if total_pages is None:
                    total_pages = data.get("total_pages", 0)
                    total_records = data.get("count", 0)
                    print(f"  API reports: {total_records} total items, {total_pages} pages")

                # Progress reporting every 10 pages
                if pages_fetched % 10 == 0:
                    elapsed = time.time() - extraction_start
                    pages_remaining = (total_pages or 1979) - page
                    avg_time_per_page = elapsed / pages_fetched
                    eta_minutes = (pages_remaining * avg_time_per_page) / 60
                    print(
                        f"  Page {page}/{total_pages or '?'} "
                        f"({len(all_results)} items, "
                        f"~{eta_minutes:.0f} min remaining)",
                        flush=True
                    )

                # Checkpoint every CHECKPOINT_INTERVAL pages
                if pages_fetched % CHECKPOINT_INTERVAL == 0:
                    self._save_items_checkpoint(all_results, page + 1, skipped_pages)

                # Check if this is the last page
                if total_pages and page >= total_pages:
                    break

                page += 1
                time.sleep(RATE_LIMIT_DELAY)

            except requests.exceptions.RequestException as e:
                print(f"\n  ERROR on page {page}: {e}")
                # Retry up to 3 times with increasing backoff
                retried = False
                for attempt in range(1, 4):
                    wait = attempt * 5
                    print(f"  Retrying in {wait}s (attempt {attempt}/3)...")
                    time.sleep(wait)
                    try:
                        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                        response.raise_for_status()
                        data = response.json()
                        results = data.get("results", [])
                        if results:
                            all_results.extend(results)
                            pages_fetched += 1
                            consecutive_failures = 0
                            print(f"  Retry succeeded (page {page}, {len(results)} items)")
                            retried = True
                            break
                    except requests.exceptions.RequestException:
                        continue
                if not retried:
                    skipped_pages.append(page)
                    consecutive_failures += 1
                    print(f"  Skipping page {page} (consecutive failures: {consecutive_failures})")

                    # Save checkpoint after every skip for safety
                    self._save_items_checkpoint(all_results, page + 1, skipped_pages)

                    # Stop if too many consecutive failures (systemic issue)
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        print(f"\n  STOPPING: {MAX_CONSECUTIVE_FAILURES} consecutive page failures.")
                        print(f"  This likely indicates a systemic issue (VPN, API down, etc.)")
                        print(f"  Collected {len(all_results)} items. Resume with --resume-items")
                        break

                    page += 1
                    time.sleep(RATE_LIMIT_DELAY * 2)  # extra delay after failure
                    continue

        elapsed_total = time.time() - extraction_start
        print(f"\n  Items extraction complete:")
        print(f"    Total items collected: {len(all_results)}")
        print(f"    Pages fetched this session: {pages_fetched}")
        print(f"    Pages skipped (failed): {len(skipped_pages)}")
        print(f"    Time elapsed: {elapsed_total / 60:.1f} minutes")

        if skipped_pages:
            print(f"    Skipped pages: {skipped_pages}")

        # Clean up checkpoint file on successful completion
        if total_pages and page >= total_pages:
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                print(f"    Checkpoint file removed (extraction complete)")

        return all_results

    def _save_items_checkpoint(self, results: List[Dict], next_page: int,
                               skipped_pages: Optional[List[int]] = None):
        """Save checkpoint data for items extraction resume."""
        checkpoint = {
            "results": results,
            "next_page": next_page,
            "timestamp": datetime.now().isoformat(),
            "total_collected": len(results),
            "skipped_pages": skipped_pages or []
        }
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False)
        print(f"    [Checkpoint saved: {len(results)} items, next page {next_page}]")

    def fetch_vocabularies(self) -> List[Dict[str, Any]]:
        """Fetch controlled vocabularies (non-paginated endpoint)."""
        url = f"{BASE_URL}/vocabularies"

        try:
            print("\nFetching vocabularies...")
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            print(f"  Total vocabularies collected: {len(results)}")
            return results

        except requests.exceptions.RequestException as e:
            print(f"  ERROR fetching vocabularies: {e}")
            return []

    def extract_all_data(self, skip_items: bool = False, resume_items: bool = False):
        """
        Extract all data from AILLA API endpoints.

        Args:
            skip_items: If True, skip the items endpoint (~50 min savings)
            resume_items: If True, resume items extraction from checkpoint
        """
        print("=" * 60)
        print("AILLA Language Atlas Data Extraction")
        print("=" * 60)
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if skip_items:
            print("Mode: Quick (skipping items endpoint)")
        elif resume_items:
            print("Mode: Resume items extraction from checkpoint")
        else:
            print("Mode: Full extraction (including ~19,785 items)")

        # Extract core datasets
        self.raw_data["languages"] = self.fetch_paginated_endpoint("/languages", "languages")
        self.raw_data["collections"] = self.fetch_paginated_endpoint("/collections", "collections")
        self.raw_data["countries"] = self.fetch_paginated_endpoint("/countries", "countries")

        # Extract items (optional, long-running)
        if not skip_items:
            self.raw_data["items"] = self.fetch_items_endpoint(resume=resume_items)
        else:
            print("\n  Skipping items extraction (--skip-items)")

        print("\n" + "=" * 60)
        print(f"Extraction complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    def save_raw_data(self, filepath: str):
        """Save raw JSON responses as backup."""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.raw_data, f, indent=2, ensure_ascii=False)
        print(f"\nRaw data saved to: {filepath}")


class AILLADataProcessor:
    """Process and structure AILLA metadata for atlas visualization."""

    def __init__(self, raw_data: Dict[str, List[Dict]]):
        self.raw_data = raw_data
        self.processed_data = {}

    def process_languages(self) -> pd.DataFrame:
        """Process languages into structured dataset."""
        print("\nProcessing languages dataset...")

        languages = []
        for lang in self.raw_data["languages"]:
            # Extract multilingual name (nested object)
            name_obj = lang.get("name", {})
            if isinstance(name_obj, dict):
                name_en = name_obj.get("en", "")
                name_es = name_obj.get("es", "")
                name_pt = name_obj.get("pt", "")
            else:
                name_en = name_es = name_pt = str(name_obj) if name_obj else ""

            # Extract multilingual description (nested object)
            desc_obj = lang.get("description", {})
            if isinstance(desc_obj, dict):
                description = desc_obj.get("en", "")
            else:
                description = str(desc_obj) if desc_obj else ""

            # Extract language family information (uses _detail field)
            family_detail = lang.get("language_family_detail", {})
            if isinstance(family_detail, dict):
                family_name_obj = family_detail.get("name", {})
                if isinstance(family_name_obj, dict):
                    family_name = family_name_obj.get("en", "")
                else:
                    family_name = str(family_name_obj) if family_name_obj else ""
                family_id = family_detail.get("id", "")
                family_code = family_detail.get("language_code", "")
            else:
                family_name = family_id = family_code = ""

            # Extract country information (uses countries_detail field)
            countries_detail = lang.get("countries_detail", [])
            countries = []
            country_codes = []
            for country in countries_detail:
                if isinstance(country, dict):
                    country_name_obj = country.get("name", {})
                    if isinstance(country_name_obj, dict):
                        countries.append(country_name_obj.get("en", ""))
                    country_codes.append(country.get("country_code", ""))

            # Extract alternative name (single string field)
            alt_name = lang.get("alternative_name", "")

            languages.append({
                "language_id": lang.get("id"),
                "name_en": name_en,
                "name_es": name_es,
                "name_pt": name_pt,
                "indigenous_name": lang.get("indigenous_name", ""),
                "alternative_name": alt_name,
                "iso_639_3_code": lang.get("language_code", ""),
                "language_family": family_name,
                "language_family_id": family_id,
                "language_family_code": family_code,
                "countries": "; ".join(countries),
                "country_codes": "; ".join(country_codes),
                "ailla_language_url": f"https://ailla.utexas.org/languages/{lang.get('id')}",
                "description": description,
                "islandora_pid": lang.get("islandora_pid", "")
            })

        df = pd.DataFrame(languages)
        print(f"  Processed {len(df)} language records")
        return df

    def process_collections(self) -> pd.DataFrame:
        """Process collections into structured dataset."""
        print("\nProcessing collections dataset...")

        collections = []
        for coll in self.raw_data["collections"]:
            # Extract multilingual title (nested object)
            title_obj = coll.get("title", {})
            if isinstance(title_obj, dict):
                title_en = title_obj.get("en", "")
                title_es = title_obj.get("es", "")
                title_pt = title_obj.get("pt", "")
            else:
                title_en = title_es = title_pt = str(title_obj) if title_obj else ""

            # Extract language information (uses collection_languages_detail)
            languages_detail = coll.get("collection_languages_detail", [])
            lang_names = []
            lang_codes = []
            for lang in languages_detail:
                if isinstance(lang, dict):
                    lang_name_obj = lang.get("name", {})
                    if isinstance(lang_name_obj, dict):
                        lang_names.append(lang_name_obj.get("en", ""))
                    lang_codes.append(lang.get("language_code", ""))

            # Extract country information (uses countries_detail)
            countries_detail = coll.get("countries_detail", [])
            countries = []
            for country in countries_detail:
                if isinstance(country, dict):
                    country_name_obj = country.get("name", {})
                    if isinstance(country_name_obj, dict):
                        countries.append(country_name_obj.get("en", ""))

            # Extract collector information (persons and organizations)
            collectors_persons = coll.get("collectors_persons_detail", [])
            collectors_orgs = coll.get("collectors_orgs_detail", [])
            collector_names = []

            for person in collectors_persons:
                if isinstance(person, dict):
                    given = person.get("given_name", "")
                    surname = person.get("surname", "")
                    full_name = f"{given} {surname}".strip()
                    if full_name:
                        collector_names.append(full_name)

            for org in collectors_orgs:
                if isinstance(org, dict):
                    org_name = org.get("name", "")
                    if org_name:
                        collector_names.append(org_name)

            collections.append({
                "collection_id": coll.get("id"),
                "title_en": title_en,
                "title_es": title_es,
                "title_pt": title_pt,
                "indigenous_title": coll.get("indigenous_title", ""),
                "languages_documented": "; ".join(lang_names),
                "language_codes": "; ".join(lang_codes),
                "countries": "; ".join(countries),
                "collectors": "; ".join(collector_names),
                "collection_url": f"https://ailla.utexas.org/collections/{coll.get('id')}",
                "islandora_pid": coll.get("islandora_pid", "")
            })

        df = pd.DataFrame(collections)
        print(f"  Processed {len(df)} collection records")
        return df

    def process_countries(self) -> pd.DataFrame:
        """Process countries into reference dataset."""
        print("\nProcessing countries dataset...")

        countries = []
        for country in self.raw_data["countries"]:
            # Extract multilingual name (nested object)
            name_obj = country.get("name", {})
            if isinstance(name_obj, dict):
                name_en = name_obj.get("en", "")
                name_es = name_obj.get("es", "")
                name_pt = name_obj.get("pt", "")
            else:
                name_en = name_es = name_pt = str(name_obj) if name_obj else ""

            countries.append({
                "country_id": country.get("id"),
                "name_en": name_en,
                "name_es": name_es,
                "name_pt": name_pt,
                "iso_country_code": country.get("country_code", ""),
                "viaf_url": country.get("viaf_url", "")
            })

        df = pd.DataFrame(countries)
        print(f"  Processed {len(df)} country records")
        return df

    def process_items(self) -> pd.DataFrame:
        """
        Process items into structured dataset with temporal data.

        Extracts date_created fields, parses YYYYMMDD format, and links
        items to collections and languages. Placeholder dates (year 1000,
        i.e. 10000101) are parsed but flagged for exclusion from range
        calculations.

        Returns:
            DataFrame with processed item records
        """
        print("\nProcessing items dataset...")

        if not self.raw_data.get("items"):
            print("  No items data to process (skipped or not extracted)")
            return pd.DataFrame()

        items = []
        for item in self.raw_data["items"]:
            # Extract multilingual name
            name_obj = item.get("name", {})
            if isinstance(name_obj, dict):
                name_en = name_obj.get("en", "")
                name_es = name_obj.get("es", "")
                name_pt = name_obj.get("pt", "")
            else:
                name_en = name_es = name_pt = str(name_obj) if name_obj else ""

            # Parse date_created (YYYYMMDD format)
            date_raw = str(item.get("date_created", ""))
            date_year = ""
            date_parsed = ""
            if date_raw and len(date_raw) == 8 and date_raw.isdigit():
                year = int(date_raw[:4])
                month = date_raw[4:6]
                day = date_raw[6:8]
                date_year = str(year)
                # Exclude placeholder year 1000 from parsed dates
                if year != 1000:
                    date_parsed = f"{date_raw[:4]}-{month}-{day}"

            # Extract collection info from collection_item_id
            # Note: the items endpoint returns collection_item_id as a detail
            # object directly (with id, en, es, pt), not as a separate _detail field
            collection_obj = item.get("collection_item_id", {})
            if isinstance(collection_obj, dict):
                collection_id = collection_obj.get("id", "")
                collection_name_en = collection_obj.get("en", "")
            else:
                collection_id = str(collection_obj) if collection_obj else ""
                collection_name_en = ""

            # Extract subject languages
            # Note: the items endpoint returns subject_languages as a list of
            # detail dicts (with id, name, language_code), not as IDs with a
            # separate _detail field
            subject_langs = item.get("subject_languages", [])
            lang_codes = []
            if isinstance(subject_langs, list):
                for lang in subject_langs:
                    if isinstance(lang, dict):
                        code = lang.get("language_code", "")
                        if code:
                            lang_codes.append(code)
                    elif lang:
                        lang_codes.append(str(lang))

            # Extract genre
            genre_detail = item.get("genre_detail", [])
            genres = []
            if isinstance(genre_detail, list):
                for g in genre_detail:
                    if isinstance(g, dict):
                        g_name = g.get("name", {})
                        if isinstance(g_name, dict):
                            genres.append(g_name.get("en", ""))
                        elif g_name:
                            genres.append(str(g_name))
            elif isinstance(genre_detail, dict):
                g_name = genre_detail.get("name", {})
                if isinstance(g_name, dict):
                    genres.append(g_name.get("en", ""))

            items.append({
                "item_id": item.get("id", ""),
                "name_en": name_en,
                "name_es": name_es,
                "name_pt": name_pt,
                "date_created": date_raw,
                "date_created_year": date_year,
                "date_created_parsed": date_parsed,
                "collection_id": collection_id,
                "collection_name_en": collection_name_en,
                "folder_id": item.get("parent_folder", ""),
                "subject_language_codes": "; ".join(lang_codes),
                "genre": "; ".join(genres),
                "visibility": item.get("visibility", ""),
                "islandora_pid": item.get("islandora_pid", ""),
                "ailla_item_url": f"https://ailla.utexas.org/items/{item.get('id', '')}"
            })

        df = pd.DataFrame(items)
        print(f"  Processed {len(df)} item records")

        # Date statistics
        if len(df) > 0 and "date_created_year" in df.columns:
            has_year = (df["date_created_year"] != "").sum()
            valid_dates = (df["date_created_parsed"] != "").sum()
            placeholder_dates = has_year - valid_dates
            print(f"  Items with date_created: {has_year}")
            print(f"  Items with valid dates (non-placeholder): {valid_dates}")
            print(f"  Items with placeholder date (year 1000): {placeholder_dates}")

        return df

    def compute_collection_date_ranges(self, items_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate item dates to the collection level.

        For each collection, computes:
        - earliest_year: Earliest recording/creation year (excluding placeholder 1000)
        - latest_year: Latest recording/creation year (excluding placeholder 1000)
        - total_items: Total number of items in the collection
        - items_with_dates: Number of items with valid (non-placeholder) dates

        Args:
            items_df: Processed items DataFrame

        Returns:
            DataFrame with one row per collection and date range columns
        """
        print("\nComputing collection date ranges...")

        if items_df.empty:
            print("  No items data available for date range computation")
            return pd.DataFrame()

        # Filter to items with valid (non-placeholder) years
        items_with_years = items_df[items_df["date_created_parsed"] != ""].copy()
        items_with_years["year_int"] = items_with_years["date_created_year"].astype(int)

        # Aggregate by collection
        date_ranges = []
        for coll_id, group in items_df.groupby("collection_id"):
            valid_years = group[group["date_created_parsed"] != ""]
            year_values = valid_years["date_created_year"].astype(int) if len(valid_years) > 0 else pd.Series(dtype=int)

            # Get collection name from first row
            coll_name = group["collection_name_en"].iloc[0] if len(group) > 0 else ""

            date_ranges.append({
                "collection_id": coll_id,
                "collection_name_en": coll_name,
                "earliest_year": int(year_values.min()) if len(year_values) > 0 else "",
                "latest_year": int(year_values.max()) if len(year_values) > 0 else "",
                "year_span": int(year_values.max()) - int(year_values.min()) if len(year_values) > 0 else "",
                "total_items": len(group),
                "items_with_dates": len(valid_years),
                "collection_url": f"https://ailla.utexas.org/collections/{coll_id}"
            })

        df = pd.DataFrame(date_ranges)
        df = df.sort_values("collection_id").reset_index(drop=True)

        print(f"  Computed date ranges for {len(df)} collections")
        if len(df) > 0:
            has_dates = (df["earliest_year"] != "").sum()
            print(f"  Collections with valid date ranges: {has_dates}")

        return df

    def augment_languages_with_dates(self, languages_df: pd.DataFrame,
                                     items_df: pd.DataFrame) -> pd.DataFrame:
        """
        Roll up temporal data from items to the language level.

        For each language (by ISO code), computes earliest_item_year,
        latest_item_year, and total_items from the items dataset. This
        fulfills the fellowship proposal's requirement for "earliest
        deposit dates."

        Placeholder dates (year 1000) are excluded from range calculations.

        Args:
            languages_df: Processed languages DataFrame
            items_df: Processed items DataFrame

        Returns:
            Languages DataFrame augmented with temporal fields
        """
        print("\nAugmenting languages with temporal data from items...")

        if items_df.empty:
            print("  No items data available for temporal augmentation")
            languages_df["earliest_item_year"] = ""
            languages_df["latest_item_year"] = ""
            languages_df["total_items"] = 0
            return languages_df

        # Build language-to-dates mapping from items
        lang_dates: Dict[str, List[int]] = {}
        lang_item_counts: Dict[str, int] = {}

        for _, item in items_df.iterrows():
            codes_str = str(item["subject_language_codes"])
            if not codes_str or codes_str == "nan":
                continue
            codes = [c.strip() for c in codes_str.split(";")]

            for code in codes:
                if not code:
                    continue
                # Count all items for this language
                lang_item_counts[code] = lang_item_counts.get(code, 0) + 1

                # Only use valid (non-placeholder) dates for ranges
                date_parsed = str(item["date_created_parsed"])
                year_str = str(item["date_created_year"])
                if date_parsed and date_parsed != "nan" and date_parsed != "":
                    try:
                        year = int(year_str)
                        if code not in lang_dates:
                            lang_dates[code] = []
                        lang_dates[code].append(year)
                    except (ValueError, TypeError):
                        pass

        # Add temporal fields to languages dataframe
        languages_df["earliest_item_year"] = languages_df["iso_639_3_code"].apply(
            lambda x: min(lang_dates[x]) if x in lang_dates else ""
        )
        languages_df["latest_item_year"] = languages_df["iso_639_3_code"].apply(
            lambda x: max(lang_dates[x]) if x in lang_dates else ""
        )
        languages_df["total_items"] = languages_df["iso_639_3_code"].apply(
            lambda x: lang_item_counts.get(x, 0)
        )

        langs_with_dates = (languages_df["earliest_item_year"] != "").sum()
        langs_with_items = (languages_df["total_items"] > 0).sum()
        print(f"  Languages with item date ranges: {langs_with_dates}")
        print(f"  Languages with any items: {langs_with_items}")
        print(f"  Total items linked to languages: {sum(lang_item_counts.values())}")

        return languages_df

    def extract_language_families(self, languages_df: pd.DataFrame) -> pd.DataFrame:
        """Extract distinct language families from languages dataset."""
        print("\nExtracting language families...")

        families = languages_df[["language_family_id", "language_family"]].drop_duplicates()
        families = families[families["language_family"] != ""]  # Remove empty families
        families = families.rename(columns={
            "language_family_id": "family_id",
            "language_family": "family_name"
        })
        families = families.sort_values("family_name").reset_index(drop=True)

        print(f"  Extracted {len(families)} distinct language families")
        return families

    def augment_languages_with_collections(self, languages_df: pd.DataFrame,
                                          collections_df: pd.DataFrame) -> pd.DataFrame:
        """Add collection counts and URLs to language dataset."""
        print("\nAugmenting languages with collection information...")

        # Build collection data per language
        lang_collections = {}

        for _, coll in collections_df.iterrows():
            lang_codes = str(coll["language_codes"]).split("; ")
            coll_url = coll["collection_url"]

            for code in lang_codes:
                code = code.strip()
                if code and code != "nan":
                    if code not in lang_collections:
                        lang_collections[code] = []
                    lang_collections[code].append(coll_url)

        # Add to languages dataframe
        languages_df["collection_count"] = languages_df["iso_639_3_code"].apply(
            lambda x: len(lang_collections.get(x, []))
        )
        languages_df["collection_urls"] = languages_df["iso_639_3_code"].apply(
            lambda x: "; ".join(lang_collections.get(x, []))
        )

        print(f"  Added collection data to {len(languages_df)} languages")
        return languages_df

    def process_all(self, include_items: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Process all datasets.

        Args:
            include_items: If True, process items and compute temporal data
        """
        print("\n" + "=" * 60)
        print("Processing and Structuring Data")
        print("=" * 60)

        # Process core datasets
        languages_df = self.process_languages()
        collections_df = self.process_collections()
        countries_df = self.process_countries()
        families_df = self.extract_language_families(languages_df)

        # Augment languages with collection information
        languages_df = self.augment_languages_with_collections(languages_df, collections_df)

        self.processed_data = {
            "languages": languages_df,
            "collections": collections_df,
            "countries": countries_df,
            "language_families": families_df
        }

        # Process items and temporal data if available
        if include_items and self.raw_data.get("items"):
            items_df = self.process_items()
            self.processed_data["items"] = items_df

            if not items_df.empty:
                # Compute collection date ranges
                collection_dates_df = self.compute_collection_date_ranges(items_df)
                self.processed_data["collection_date_ranges"] = collection_dates_df

                # Augment languages with temporal data
                languages_df = self.augment_languages_with_dates(languages_df, items_df)
                self.processed_data["languages"] = languages_df
        elif include_items:
            print("\n  No items data available - skipping temporal processing")

        return self.processed_data

    def save_csv_files(self, output_dir: str):
        """Save all datasets as CSV files."""
        print("\n" + "=" * 60)
        print("Saving CSV Files")
        print("=" * 60)

        for name, df in self.processed_data.items():
            filepath = f"{output_dir}/{name}_dataset.csv"
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"  Saved: {filepath} ({len(df)} records)")

    def save_json_file(self, filepath: str):
        """Save complete structured JSON export."""
        print("\n" + "=" * 60)
        print("Saving Structured JSON")
        print("=" * 60)

        json_data = {
            "languages": self.processed_data["languages"].to_dict(orient="records"),
            "collections": self.processed_data["collections"].to_dict(orient="records"),
            "countries": self.processed_data["countries"].to_dict(orient="records"),
            "language_families": self.processed_data["language_families"].to_dict(orient="records"),
            "metadata": {
                "extraction_date": datetime.now().isoformat(),
                "total_languages": len(self.processed_data["languages"]),
                "total_collections": len(self.processed_data["collections"]),
                "total_countries": len(self.processed_data["countries"]),
                "total_language_families": len(self.processed_data["language_families"]),
                "data_source": "https://ailla.utexas.org",
                "scraper_version": "2.0",
                "project": "AILLA Language Atlas (LBDS Fellowship)"
            }
        }

        # Include items and collection date ranges if available
        if "items" in self.processed_data and not self.processed_data["items"].empty:
            json_data["items"] = self.processed_data["items"].to_dict(orient="records")
            json_data["metadata"]["total_items"] = len(self.processed_data["items"])
        if "collection_date_ranges" in self.processed_data and not self.processed_data["collection_date_ranges"].empty:
            json_data["collection_date_ranges"] = self.processed_data["collection_date_ranges"].to_dict(orient="records")

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

        print(f"  Saved: {filepath}")

    def generate_summary_report(self) -> str:
        """Generate data quality and completeness summary."""
        report = []
        report.append("\n" + "=" * 60)
        report.append("DATA QUALITY SUMMARY REPORT")
        report.append("=" * 60)

        languages_df = self.processed_data["languages"]
        collections_df = self.processed_data["collections"]

        # Overall counts
        report.append(f"\nDataset Sizes:")
        report.append(f"  Languages: {len(languages_df)}")
        report.append(f"  Collections: {len(collections_df)}")
        report.append(f"  Countries: {len(self.processed_data['countries'])}")
        report.append(f"  Language Families: {len(self.processed_data['language_families'])}")
        if "items" in self.processed_data and not self.processed_data["items"].empty:
            report.append(f"  Items: {len(self.processed_data['items'])}")

        # Language completeness
        report.append(f"\nLanguage Data Completeness:")
        report.append(f"  Languages with ISO 639-3 codes: {languages_df['iso_639_3_code'].notna().sum()}")
        report.append(f"  Languages missing ISO codes: {languages_df['iso_639_3_code'].isna().sum()}")
        report.append(f"  Languages with family classification: {(languages_df['language_family'] != '').sum()}")
        report.append(f"  Languages with geographic data: {(languages_df['countries'] != '').sum()}")
        report.append(f"  Languages with indigenous names: {(languages_df['indigenous_name'] != '').sum()}")

        # Collection linkage
        report.append(f"\nCollection Documentation:")
        report.append(f"  Languages with collections: {(languages_df['collection_count'] > 0).sum()}")
        report.append(f"  Languages without collections: {(languages_df['collection_count'] == 0).sum()}")
        report.append(f"  Average collections per language: {languages_df['collection_count'].mean():.2f}")
        report.append(f"  Max collections for one language: {languages_df['collection_count'].max()}")

        # Temporal data (if items were processed)
        if "total_items" in languages_df.columns:
            report.append(f"\nTemporal Data (from Items):")
            langs_with_items = (languages_df["total_items"] > 0).sum()
            langs_with_dates = (languages_df["earliest_item_year"] != "").sum()
            report.append(f"  Languages with item records: {langs_with_items}")
            report.append(f"  Languages with date ranges: {langs_with_dates}")
            if langs_with_dates > 0:
                valid_years = languages_df[languages_df["earliest_item_year"] != ""]["earliest_item_year"]
                report.append(f"  Earliest recording year across all languages: {int(valid_years.astype(int).min())}")
                valid_latest = languages_df[languages_df["latest_item_year"] != ""]["latest_item_year"]
                report.append(f"  Latest recording year across all languages: {int(valid_latest.astype(int).max())}")

        if "collection_date_ranges" in self.processed_data and not self.processed_data["collection_date_ranges"].empty:
            cdr = self.processed_data["collection_date_ranges"]
            has_dates = cdr[cdr["earliest_year"] != ""]
            report.append(f"\nCollection Date Ranges:")
            report.append(f"  Collections with date ranges: {len(has_dates)}/{len(cdr)}")
            if len(has_dates) > 0:
                report.append(f"  Earliest collection year: {int(has_dates['earliest_year'].astype(int).min())}")
                report.append(f"  Latest collection year: {int(has_dates['latest_year'].astype(int).max())}")

        # Top language families
        report.append(f"\nTop 10 Language Families by Number of Languages:")
        family_counts = languages_df[languages_df['language_family'] != '']['language_family'].value_counts().head(10)
        for family, count in family_counts.items():
            report.append(f"  {family}: {count} languages")

        # Geographic distribution
        report.append(f"\nTop 10 Countries by Number of Languages:")
        all_countries = []
        for countries_str in languages_df['countries']:
            if countries_str and countries_str != '':
                all_countries.extend([c.strip() for c in str(countries_str).split(';')])
        country_counts = pd.Series(all_countries).value_counts().head(10)
        for country, count in country_counts.items():
            report.append(f"  {country}: {count} languages")

        report.append("\n" + "=" * 60)

        return "\n".join(report)


def compare_api_to_ailla2(items_csv_path: str = "data/items_dataset.csv") -> pd.DataFrame:
    """Compare API-extracted items against AILLA2 spreadsheets.

    Uses a composite key of (Name EN + normalized Date Created) to match items
    between the two sources. Items found in the API but not in AILLA2 are
    likely added after the AILLA2 export dates (Feb-Oct 2024).

    Args:
        items_csv_path: Path to the API-extracted items CSV.

    Returns:
        DataFrame of API-only items (not found in AILLA2).
    """
    print("\n" + "=" * 60)
    print("Comparing API Items vs AILLA2 Spreadsheets")
    print("=" * 60)

    # Load API items
    if not os.path.exists(items_csv_path):
        print(f"  ERROR: {items_csv_path} not found.")
        print("  Run extraction first (without --skip-items) to generate items data.")
        return pd.DataFrame()

    api_items = pd.read_csv(items_csv_path, dtype=str).fillna("")
    print(f"  API items loaded: {len(api_items)}")

    # Load AILLA2 Items sheets from all 7 Excel files
    ailla2_dir = Path("AILLA2")
    ailla2_pattern = "all-MODS-priority-*.xlsx"
    files = sorted(ailla2_dir.glob(ailla2_pattern))
    files = [f for f in files if not f.name.startswith("~$")]

    if not files:
        print(f"  ERROR: No AILLA2 files found in {ailla2_dir}/")
        return pd.DataFrame()

    print(f"  Loading {len(files)} AILLA2 files...")
    all_ailla2_items = []
    for filepath in files:
        items_df = pd.read_excel(filepath, sheet_name="Items", dtype=str).fillna("")
        all_ailla2_items.append(items_df)
        print(f"    {filepath.name}: {len(items_df)} items")

    ailla2_items = pd.concat(all_ailla2_items, ignore_index=True)
    print(f"  AILLA2 items loaded: {len(ailla2_items)}")

    # Build composite keys: name_en + "|" + normalized date
    def normalize_date(date_str: str) -> str:
        """Normalize date to YYYYMMDD format for matching."""
        date_str = str(date_str).strip()
        # Already YYYYMMDD (8 digits)
        if len(date_str) == 8 and date_str.isdigit():
            return date_str
        # YYYY-MM-DD format
        digits = date_str.replace("-", "").replace("/", "")
        if len(digits) == 8 and digits.isdigit():
            return digits
        return date_str

    # API keys: use name_en and date_created columns
    api_keys = set()
    api_key_to_rows: Dict[str, List[int]] = {}
    for idx, row in api_items.iterrows():
        name = str(row.get("name_en", "")).strip()
        date = normalize_date(str(row.get("date_created", "")))
        key = f"{name}|{date}"
        api_keys.add(key)
        if key not in api_key_to_rows:
            api_key_to_rows[key] = []
        api_key_to_rows[key].append(idx)

    # AILLA2 keys: use "Name EN" and "Date Created" columns
    ailla2_keys = set()
    for _, row in ailla2_items.iterrows():
        name = str(row.get("Name EN", "")).strip()
        date = normalize_date(str(row.get("Date Created", "")))
        key = f"{name}|{date}"
        ailla2_keys.add(key)

    # Find API-only keys
    api_only_keys = api_keys - ailla2_keys
    matched_keys = api_keys & ailla2_keys

    print(f"\n  Matching results:")
    print(f"    Unique API keys: {len(api_keys)}")
    print(f"    Unique AILLA2 keys: {len(ailla2_keys)}")
    print(f"    Matched: {len(matched_keys)} ({len(matched_keys)/len(api_keys)*100:.1f}%)")
    print(f"    API-only (not in AILLA2): {len(api_only_keys)}")

    # Collect API-only rows
    api_only_indices = []
    for key in api_only_keys:
        api_only_indices.extend(api_key_to_rows.get(key, []))

    api_only_df = api_items.iloc[api_only_indices].copy()
    api_only_df = api_only_df.sort_values(["collection_name_en", "name_en"]).reset_index(drop=True)

    # Summary by collection
    if not api_only_df.empty:
        print(f"\n  API-only items by collection:")
        coll_counts = api_only_df["collection_name_en"].value_counts()
        for coll_name, count in coll_counts.head(15).items():
            print(f"    {coll_name}: {count} items")
        if len(coll_counts) > 15:
            print(f"    ... and {len(coll_counts) - 15} more collections")

    # Save output
    output_path = "data/api_only_items.csv"
    api_only_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"\n  Saved: {output_path} ({len(api_only_df)} items)")

    return api_only_df


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="AILLA Language Atlas Data Scraper - Extract metadata from AILLA's public API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run scripts/ailla_scraper.py                  Full extraction (~52 min)
  uv run scripts/ailla_scraper.py --skip-items      Quick mode, skip items (~2 min)
  uv run scripts/ailla_scraper.py --resume-items     Resume after interruption
  uv run scripts/ailla_scraper.py --compare-ailla2   Compare API items vs AILLA2
        """
    )
    parser.add_argument(
        "--skip-items",
        action="store_true",
        help="Skip the items endpoint extraction (~50 min savings). "
             "Extracts only languages, collections, and countries."
    )
    parser.add_argument(
        "--resume-items",
        action="store_true",
        help="Resume items extraction from the last checkpoint. "
             "Use after an interrupted full extraction."
    )
    parser.add_argument(
        "--compare-ailla2",
        action="store_true",
        help="After extraction, compare API items against AILLA2 spreadsheets "
             "to find items only in the API (added post-AILLA2 export). "
             "Outputs data/api_only_items.csv for manual review."
    )
    return parser.parse_args()


def main():
    """Main execution function."""
    args = parse_args()

    # Step 1: Extract data
    scraper = AILLAScraper()
    scraper.extract_all_data(
        skip_items=args.skip_items,
        resume_items=args.resume_items
    )

    # Save raw data as backup
    scraper.save_raw_data("data/ailla_raw_data.json")

    # Step 2: Process data
    processor = AILLADataProcessor(scraper.raw_data)
    include_items = not args.skip_items and bool(scraper.raw_data.get("items"))
    processor.process_all(include_items=include_items)

    # Step 3: Save outputs
    processor.save_csv_files("data")
    processor.save_json_file("data/ailla_atlas_data.json")

    # Step 4: Generate summary report
    report = processor.generate_summary_report()
    print(report)

    # Save report to file
    with open("data/extraction_report.txt", 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\nFull report saved to: data/extraction_report.txt")

    # Step 5: Compare API items vs AILLA2 (optional)
    if args.compare_ailla2:
        compare_api_to_ailla2("data/items_dataset.csv")

    print("\n" + "=" * 60)
    print("ALL TASKS COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    main()
