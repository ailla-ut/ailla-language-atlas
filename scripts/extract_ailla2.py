#!/usr/bin/env python3
"""Extract item-level data from AILLA2 pre-migration spreadsheets.

Reads all 7 AILLA2 Excel files (AILLA2/all-MODS-priority-*.xlsx) and computes
corrected item counts, date ranges, and collection statistics for each language.
Updates data/languages_dataset.csv with these values, replacing the incomplete
data from the API extraction (which captured only 4,530 of ~18,548 items).

Counts ALL items regardless of visibility (RST, EMB, PUB, LOG) for total_items,
since restricted/embargoed metadata is visible on the AILLA website. A separate
public_items count (PUB + LOG only) is computed for threshold and display logic
in the StoryMap pipeline.

The key data flow (hybrid two-pass counting):
  Pass 1: Files -> Media Languages (per-file language IDs, most precise)
  Pass 2: For folders missing from the Files sheet, fall back to
          Items -> Folders -> Subject Languages (folder-level attribution)
  Meta-languages (English, Spanish, Portuguese, "No linguistic content")
  are excluded from Media Languages counts.

Usage:
    uv run scripts/extract_ailla2.py

Output:
    - Updates data/languages_dataset.csv (total_items, public_items, date ranges, deposit dates)
    - Generates data/ailla2_verification_report.txt (comparison report)
"""

import ast
import re
from pathlib import Path

import pandas as pd


# --- Configuration ---

AILLA2_DIR = Path("AILLA2")
AILLA2_PATTERN = "all-MODS-priority-*.xlsx"
DATA_DIR = Path("data")
LANGUAGES_CSV = DATA_DIR / "languages_dataset.csv"
REPORT_FILE = DATA_DIR / "ailla2_verification_report.txt"

# Meta-language IDs to exclude from file-level Media Languages counting.
# These appear in files as translation/annotation languages, not as the
# documented indigenous language.
META_LANGUAGE_IDS = {8, 9, 399, 641}  # English, Spanish, Portuguese, No linguistic content


def load_ailla2_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load Items and Folders sheets from all AILLA2 Excel files.

    Reads each priority file's Items and Folders sheets, combines them into
    two DataFrames. Skips temporary/lock files (starting with ~$).

    Returns:
        Tuple of (all_items, all_folders) DataFrames.
    """
    files = sorted(AILLA2_DIR.glob(AILLA2_PATTERN))
    files = [f for f in files if not f.name.startswith("~$")]

    print(f"Found {len(files)} AILLA2 files")

    all_items = []
    all_folders = []

    for filepath in files:
        print(f"  Reading {filepath.name}...")

        items_df = pd.read_excel(filepath, sheet_name="Items")
        folders_df = pd.read_excel(filepath, sheet_name="Folders")

        # Tag with source file for traceability
        items_df["source_file"] = filepath.name
        folders_df["source_file"] = filepath.name

        # Track original row index per source file so Files' "Item Row #"
        # can look up the correct item for date ranges
        items_df["item_row_in_source"] = range(len(items_df))

        all_items.append(items_df)
        all_folders.append(folders_df)

        print(f"    {len(items_df)} items, {len(folders_df)} folders")

    items = pd.concat(all_items, ignore_index=True)
    folders = pd.concat(all_folders, ignore_index=True)

    print(f"\nTotal: {len(items)} items, {len(folders)} folders")

    # Report visibility breakdown (but keep all items)
    visibility_counts = items["Visibility"].value_counts()
    print("\nVisibility breakdown:")
    for vis, count in visibility_counts.items():
        print(f"  {vis}: {count}")

    pub_log = items["Visibility"].isin(["PUB", "LOG"]).sum()
    print(f"\nKeeping all {len(items)} items (PUB+LOG: {pub_log}, RST+EMB: {len(items) - pub_log})")

    return items, folders


def parse_subject_languages(value: object) -> list[int]:
    """Parse the Subject Languages column into a list of integer language IDs.

    The column contains string representations of Python lists, e.g.:
      '[39, 175]' -> [39, 175]
      '[134]'     -> [134]

    Args:
        value: The cell value (string, list, or NaN).

    Returns:
        List of integer language IDs, or empty list if unparseable.
    """
    if pd.isna(value):
        return []

    s = str(value).strip()
    if not s or s == "[]":
        return []

    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [int(x) for x in parsed if x is not None]
        if isinstance(parsed, (int, float)):
            return [int(parsed)]
    except (ValueError, SyntaxError):
        pass

    return []


def normalize_folder_pid(pid: str) -> str:
    """Normalize a folder PID by stripping the -res suffix.

    Items referencing restricted versions (e.g., 'ailla:256799-res')
    should map to the base folder ('ailla:256799').

    Also handles rare suffixed variants like 'ailla:245618-2'.

    Args:
        pid: The Islandora PID string.

    Returns:
        Normalized PID string.
    """
    if pd.isna(pid):
        return ""
    s = str(pid).strip()
    # Strip -res suffix
    if s.endswith("-res"):
        return s[:-4]
    # Strip numeric suffixes like -1, -2
    match = re.match(r"^(ailla:\d+)-\d+$", s)
    if match:
        return match.group(1)
    return s


def parse_year(date_str: object) -> int | None:
    """Extract a valid year from a Date Created value.

    Handles formats:
      - 'YYYY-MM-DD' (standard, possibly with zeroed unknowns: '2018-00-00')
      - 'YYYY-MM-DD HH:MM:SS' (timestamps in priority-5 file)
      - Empty/null values

    Excludes years <= 1000 (placeholders for unknown dates).

    Args:
        date_str: The date value from the spreadsheet.

    Returns:
        Integer year if valid, None otherwise.
    """
    if pd.isna(date_str):
        return None

    s = str(date_str).strip()
    if not s:
        return None

    # Extract the year (first 4 characters before the first separator)
    match = re.match(r"(\d{4})", s)
    if not match:
        return None

    year = int(match.group(1))

    # Exclude placeholder years
    if year <= 1000:
        return None

    return year


def build_folder_language_map(
    folders: pd.DataFrame,
) -> dict[str, list[int]]:
    """Map folder Islandora PIDs to their language IDs.

    Args:
        folders: Combined Folders DataFrame with 'Islandora PID' and
                 'Subject Languages' columns.

    Returns:
        Dict mapping normalized folder PID -> list of language IDs.
    """
    folder_map: dict[str, list[int]] = {}

    for _, row in folders.iterrows():
        pid = row.get("Islandora PID")
        if pd.isna(pid):
            continue

        pid_str = str(pid).strip()
        if not pid_str:
            continue

        lang_ids = parse_subject_languages(row.get("Subject Languages"))
        if lang_ids:
            folder_map[pid_str] = lang_ids

    print(f"Built folder-language map: {len(folder_map)} folders with language data")
    return folder_map


def load_files_data() -> pd.DataFrame:
    """Load Files sheets from all AILLA2 Excel files.

    Reads each priority file's Files sheet, combines into one DataFrame.
    Keeps all files regardless of visibility (RST, EMB, PUB, LOG).

    Returns:
        Combined Files DataFrame (all visibilities).
    """
    files_list = sorted(AILLA2_DIR.glob(AILLA2_PATTERN))
    files_list = [f for f in files_list if not f.name.startswith("~$")]

    print(f"\n{'='*60}")
    print("LOADING FILES SHEETS")
    print(f"{'='*60}")

    all_files = []
    for filepath in files_list:
        print(f"  Reading Files from {filepath.name}...")
        files_df = pd.read_excel(filepath, sheet_name="Files")
        files_df["source_file"] = filepath.name
        all_files.append(files_df)
        print(f"    {len(files_df)} files")

    files = pd.concat(all_files, ignore_index=True)
    print(f"\nTotal files: {len(files)}")

    # Report visibility breakdown (but keep all files)
    if "Visibility" in files.columns:
        vis_counts = files["Visibility"].value_counts()
        print("\nFiles visibility breakdown:")
        for vis, count in vis_counts.items():
            print(f"  {vis}: {count}")

        pub_log = files["Visibility"].isin(["PUB", "LOG"]).sum()
        print(f"\nKeeping all {len(files)} files (PUB+LOG: {pub_log}, RST+EMB: {len(files) - pub_log})")

    return files


def compute_deposit_stats(
    files: pd.DataFrame,
    folder_lang_map: dict[str, list[int]],
) -> pd.DataFrame:
    """Compute per-language deposit date ranges from AILLA2 Files data.

    For each file:
    1. Look up its folder's language IDs via the folder-language map
    2. Parse the file's Date Uploaded to extract the deposit year
    3. Track earliest and latest deposit year per language

    Does NOT count files per language (files:items is one-to-many,
    so file counts would be misleadingly inflated).

    Args:
        files: Combined Files DataFrame (already filtered for visibility).
        folder_lang_map: Dict mapping folder PID -> list of language IDs.

    Returns:
        DataFrame with columns: language_id, earliest_deposit_year,
        latest_deposit_year.
    """
    lang_earliest: dict[int, int] = {}
    lang_latest: dict[int, int] = {}

    matched = 0
    unmatched = 0

    for _, row in files.iterrows():
        folder_pid = normalize_folder_pid(row.get("Folder"))
        if not folder_pid:
            unmatched += 1
            continue

        lang_ids = folder_lang_map.get(folder_pid, [])
        if not lang_ids:
            unmatched += 1
            continue

        matched += 1
        year = parse_year(row.get("Date Uploaded"))

        if year is not None:
            for lid in lang_ids:
                if lid not in lang_earliest or year < lang_earliest[lid]:
                    lang_earliest[lid] = year
                if lid not in lang_latest or year > lang_latest[lid]:
                    lang_latest[lid] = year

    print(f"\nFile-to-folder matching: {matched} matched, {unmatched} unmatched")

    records = []
    for lid in sorted(set(lang_earliest.keys()) | set(lang_latest.keys())):
        records.append({
            "language_id": lid,
            "earliest_deposit_year": lang_earliest.get(lid),
            "latest_deposit_year": lang_latest.get(lid),
        })

    result = pd.DataFrame(records)
    print(f"Computed deposit stats for {len(result)} languages")
    return result


def compute_language_stats(
    items: pd.DataFrame,
    files: pd.DataFrame,
    folder_lang_map: dict[str, list[int]],
) -> pd.DataFrame:
    """Compute per-language item counts and date ranges using hybrid counting.

    Two-pass approach:
      Pass 1 (file-level): For items whose folders have Files sheet entries,
        use the per-file Media Languages column for precise language attribution.
        Meta-language IDs (English, Spanish, Portuguese, "No linguistic content")
        are excluded.
      Pass 2 (folder fallback): For items in folders with NO Files sheet entries,
        fall back to the folder's Subject Languages (less precise but preserves
        coverage for collections with incomplete Files data).

    Date ranges come from Items' Date Created in both passes.

    This correctly separates languages that share ISO codes (e.g., Mocho ID 39
    vs Tuzanteco ID 533, both ISO 'mhc') because we join on language_id.

    Args:
        items: Combined Items DataFrame (already filtered for visibility).
        files: Combined Files DataFrame (already filtered for visibility).
        folder_lang_map: Dict mapping folder PID -> list of language IDs.

    Returns:
        DataFrame with columns: language_id, ailla2_total_items,
        ailla2_earliest_year, ailla2_latest_year.
    """
    # Per-language accumulators
    lang_items: dict[int, set] = {}  # lang_id -> set of unique item keys
    lang_earliest: dict[int, int] = {}
    lang_latest: dict[int, int] = {}

    # Build set of folder PIDs that have Files sheet entries
    folders_with_files = set()
    for folder_pid in files["Folder"].dropna():
        normalized = normalize_folder_pid(folder_pid)
        if normalized:
            folders_with_files.add(normalized)

    # Build item date lookup keyed by (source_file, item_row_in_source)
    # so Files' "Item Row #" can find the correct Date Created
    item_dates: dict[tuple, int | None] = {}
    for _, row in items.iterrows():
        key = (row.get("source_file", ""), row.get("item_row_in_source"))
        item_dates[key] = parse_year(row.get("Date Created"))

    # --- Pass 1: File-level counting ---
    print("\nPass 1: File-level counting (Media Languages)")
    file_matched = 0
    file_unmatched = 0

    for _, frow in files.iterrows():
        media_langs = parse_subject_languages(frow.get("Media Languages"))
        # Filter out meta-languages
        media_langs = [lid for lid in media_langs if lid not in META_LANGUAGE_IDS]
        if not media_langs:
            file_unmatched += 1
            continue

        file_matched += 1

        # Identify unique parent item (for deduplication)
        item_key = (
            frow.get("source_file", ""),
            str(frow.get("Folder", "")).strip(),
            frow.get("Item Row #", ""),
        )

        # Get date from item lookup using (source_file, Item Row #)
        item_row = frow.get("Item Row #")
        date_key = (frow.get("source_file", ""), item_row - 2 if pd.notna(item_row) else None)
        year = item_dates.get(date_key)

        for lid in media_langs:
            if lid not in lang_items:
                lang_items[lid] = set()
            lang_items[lid].add(item_key)

            if year is not None:
                if lid not in lang_earliest or year < lang_earliest[lid]:
                    lang_earliest[lid] = year
                if lid not in lang_latest or year > lang_latest[lid]:
                    lang_latest[lid] = year

    print(f"  Files with indigenous language tags: {file_matched}")
    print(f"  Files with no indigenous language tags: {file_unmatched}")

    # --- Pass 2: Folder fallback for items in folders without Files entries ---
    print("\nPass 2: Folder-level fallback (Subject Languages)")
    fallback_matched = 0
    fallback_unmatched = 0
    fallback_skipped = 0

    for _, row in items.iterrows():
        folder_pid = normalize_folder_pid(row.get("Folder"))
        if not folder_pid:
            fallback_unmatched += 1
            continue

        # Skip items in folders that HAVE Files entries (already counted in Pass 1)
        if folder_pid in folders_with_files:
            fallback_skipped += 1
            continue

        lang_ids = folder_lang_map.get(folder_pid, [])
        if not lang_ids:
            fallback_unmatched += 1
            continue

        fallback_matched += 1
        year = parse_year(row.get("Date Created"))

        # Use a unique key for this item (distinct from file-level keys)
        item_key = (
            row.get("source_file", ""),
            str(row.get("Folder", "")).strip(),
            f"fallback_{row.name}",  # use DataFrame index for uniqueness
        )

        for lid in lang_ids:
            if lid not in lang_items:
                lang_items[lid] = set()
            lang_items[lid].add(item_key)

            if year is not None:
                if lid not in lang_earliest or year < lang_earliest[lid]:
                    lang_earliest[lid] = year
                if lid not in lang_latest or year > lang_latest[lid]:
                    lang_latest[lid] = year

    print(f"  Items counted via folder fallback: {fallback_matched}")
    print(f"  Items skipped (already file-counted): {fallback_skipped}")
    print(f"  Items unmatched: {fallback_unmatched}")

    # Build results DataFrame (convert sets to counts)
    records = []
    for lid in sorted(lang_items.keys()):
        records.append({
            "language_id": lid,
            "ailla2_total_items": len(lang_items[lid]),
            "ailla2_earliest_year": lang_earliest.get(lid),
            "ailla2_latest_year": lang_latest.get(lid),
        })

    result = pd.DataFrame(records)
    total_items = sum(len(s) for s in lang_items.values())
    print(f"\nComputed stats for {len(result)} languages ({total_items} total item-language pairs)")
    return result


def compute_public_items(
    items: pd.DataFrame,
    files: pd.DataFrame,
    folder_lang_map: dict[str, list[int]],
) -> pd.DataFrame:
    """Compute per-language item counts using only PUB+LOG items/files.

    Uses the same hybrid counting logic as compute_language_stats() but
    filters to publicly accessible items only. Returns just the item counts
    (no date ranges needed since those use all items).

    Args:
        items: Combined Items DataFrame (all visibilities).
        files: Combined Files DataFrame (all visibilities).
        folder_lang_map: Dict mapping folder PID -> list of language IDs.

    Returns:
        DataFrame with columns: language_id, public_items.
    """
    print(f"\n{'='*60}")
    print("COMPUTING PUBLIC-ONLY ITEM COUNTS (PUB + LOG)")
    print(f"{'='*60}")

    # Filter to public items and files
    pub_items = items[items["Visibility"].isin(["PUB", "LOG"])].copy()
    pub_files = files[files["Visibility"].isin(["PUB", "LOG"])].copy()
    print(f"Public items: {len(pub_items)}, Public files: {len(pub_files)}")

    # Run hybrid counting on public-only data (reuse compute_language_stats)
    pub_stats = compute_language_stats(pub_items, pub_files, folder_lang_map)

    # Rename to public_items
    result = pub_stats[["language_id", "ailla2_total_items"]].copy()
    result.rename(columns={"ailla2_total_items": "public_items"}, inplace=True)

    return result


def update_languages_csv(
    lang_stats: pd.DataFrame,
    public_stats: pd.DataFrame,
    deposit_stats: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Update languages_dataset.csv with AILLA2-derived item and deposit data.

    Replaces total_items, earliest_item_year, and latest_item_year for all
    languages. Adds public_items (PUB+LOG only) and deposit date columns.
    Languages not found in AILLA2 data get zeroed out (they have no items
    in the spreadsheets).

    Args:
        lang_stats: DataFrame from compute_language_stats() with ailla2_* columns.
        public_stats: DataFrame from compute_public_items() with public_items column.
        deposit_stats: DataFrame from compute_deposit_stats() with deposit columns.

    Returns:
        Tuple of (updated languages DataFrame, comparison DataFrame for reporting).
    """
    languages = pd.read_csv(LANGUAGES_CSV)
    print(f"\nLoaded {len(languages)} languages from {LANGUAGES_CSV}")

    # Save old values for comparison
    comparison = languages[
        ["language_id", "name_en", "language_family", "total_items",
         "earliest_item_year", "latest_item_year"]
    ].copy()
    comparison.rename(columns={
        "total_items": "api_total_items",
        "earliest_item_year": "api_earliest_year",
        "latest_item_year": "api_latest_year",
    }, inplace=True)

    # Merge AILLA2 stats
    languages = languages.merge(
        lang_stats[["language_id", "ailla2_total_items", "ailla2_earliest_year", "ailla2_latest_year"]],
        on="language_id",
        how="left",
    )

    # Replace old values with AILLA2 values
    languages["total_items"] = languages["ailla2_total_items"].fillna(0).astype(int)
    languages["earliest_item_year"] = languages["ailla2_earliest_year"]
    languages["latest_item_year"] = languages["ailla2_latest_year"]

    # Manual date overrides for known Media Languages tagging errors in source data.
    # Yauyos Quechua (272): files in folders ailla:135099 and ailla:135122 are
    # incorrectly tagged with language_id 272, pulling in 1978 dates from
    # Mapuche/Tucanoan collections. Actual Shimelman collection starts in 2001.
    date_overrides = {
        272: {"earliest_item_year": 2001, "latest_item_year": 2014},
    }
    for lang_id, overrides in date_overrides.items():
        mask = languages["language_id"] == lang_id
        for col, val in overrides.items():
            languages.loc[mask, col] = val
        print(f"  Date override applied: language_id {lang_id} -> {overrides}")

    # Drop temporary columns
    languages.drop(
        columns=["ailla2_total_items", "ailla2_earliest_year", "ailla2_latest_year"],
        inplace=True,
    )

    # Drop pre-existing public_items and deposit columns for idempotent reruns
    for col in ["public_items", "earliest_deposit_year", "latest_deposit_year"]:
        if col in languages.columns:
            languages.drop(columns=[col], inplace=True)

    # Merge public-only item counts
    languages = languages.merge(
        public_stats[["language_id", "public_items"]],
        on="language_id",
        how="left",
    )
    languages["public_items"] = languages["public_items"].fillna(0).astype(int)

    # Manual item count overrides for languages where AILLA2 spreadsheets undercount
    # (items deposited after the AILLA2 export).
    # Ancash Huayllas Quechua (133): AILLA2 has 5 items, live site shows 33.
    # Verified against live AILLA site 2026-03-24.
    item_overrides = {
        133: {"total_items": 33, "public_items": 33},
    }
    for lang_id, overrides in item_overrides.items():
        mask = languages["language_id"] == lang_id
        for col, val in overrides.items():
            languages.loc[mask, col] = val
        print(f"  Item override applied: language_id {lang_id} -> {overrides}")

    # Metadata overrides for fields missing from the API.
    # Ixil (46) and Inga (27) have no countries in the API response.
    metadata_overrides = {
        46: {"countries": "Guatemala", "country_codes": "GTM"},
        27: {"countries": "Colombia", "country_codes": "COL"},
    }
    for lang_id, overrides in metadata_overrides.items():
        mask = languages["language_id"] == lang_id
        for col, val in overrides.items():
            languages.loc[mask, col] = val
        print(f"  Metadata override applied: language_id {lang_id} -> {overrides}")

    # Merge deposit stats
    languages = languages.merge(
        deposit_stats[["language_id", "earliest_deposit_year", "latest_deposit_year"]],
        on="language_id",
        how="left",
    )

    # Reorder: place public_items and deposit columns after total_items
    cols = list(languages.columns)
    for col in ["public_items", "earliest_deposit_year", "latest_deposit_year"]:
        cols.remove(col)
    total_items_idx = cols.index("total_items")
    cols.insert(total_items_idx + 1, "public_items")
    cols.insert(total_items_idx + 2, "earliest_deposit_year")
    cols.insert(total_items_idx + 3, "latest_deposit_year")
    languages = languages[cols]

    # Add AILLA2 values to comparison
    comparison = comparison.merge(
        lang_stats[["language_id", "ailla2_total_items", "ailla2_earliest_year", "ailla2_latest_year"]],
        on="language_id",
        how="left",
    )
    comparison["ailla2_total_items"] = comparison["ailla2_total_items"].fillna(0).astype(int)

    # Add deposit stats to comparison for reporting
    comparison = comparison.merge(
        deposit_stats[["language_id", "earliest_deposit_year", "latest_deposit_year"]],
        on="language_id",
        how="left",
    )

    # Save updated CSV
    languages.to_csv(LANGUAGES_CSV, index=False, encoding="utf-8")
    print(f"Updated {LANGUAGES_CSV}")

    # Summary
    has_items = (languages["total_items"] > 0).sum()
    has_dates = languages["earliest_item_year"].notna().sum()
    has_deposit = languages["earliest_deposit_year"].notna().sum()
    print(f"  Languages with items: {has_items}")
    print(f"  Languages with date ranges: {has_dates}")
    print(f"  Languages with deposit dates: {has_deposit}")
    print(f"  Total items across all languages: {languages['total_items'].sum()}")
    print(f"  Public items across all languages: {languages['public_items'].sum()}")

    return languages, comparison


def generate_report(
    comparison: pd.DataFrame,
    items: pd.DataFrame,
    folders: pd.DataFrame,
    languages: pd.DataFrame,
    files: pd.DataFrame,
) -> None:
    """Generate a verification report comparing API vs AILLA2 data.

    Outputs per-language comparison, collection matching stats, flagged
    discrepancies, deposit date statistics, and summary statistics.

    Args:
        comparison: DataFrame with api_*, ailla2_*, and deposit columns.
        items: Combined Items DataFrame (for total counts).
        folders: Combined Folders DataFrame (for total counts).
        languages: Updated languages DataFrame.
        files: Combined Files DataFrame (for deposit date stats).
    """
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("AILLA2 DATA VERIFICATION REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Summary statistics
    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"AILLA2 source files: 7 Excel workbooks")
    lines.append(f"Total items in AILLA2 (all visibilities): {len(items)}")
    pub_log_items = items["Visibility"].isin(["PUB", "LOG"]).sum()
    lines.append(f"  PUB+LOG items: {pub_log_items}")
    lines.append(f"  RST+EMB items: {len(items) - pub_log_items}")
    lines.append(f"Total folders in AILLA2: {len(folders)}")
    lines.append(f"Languages with AILLA2 items: {(comparison['ailla2_total_items'] > 0).sum()}")
    lines.append(f"Total AILLA2 items assigned to languages: {comparison['ailla2_total_items'].sum()}")
    lines.append("")

    api_total = comparison["api_total_items"].fillna(0).astype(int).sum()
    ailla2_total = comparison["ailla2_total_items"].sum()
    lines.append(f"API extraction total items: {api_total}")
    lines.append(f"AILLA2 spreadsheet total items: {ailla2_total}")
    if api_total > 0:
        pct_increase = ((ailla2_total - api_total) / api_total) * 100
        lines.append(f"Change: {ailla2_total - api_total:+d} items ({pct_increase:+.1f}%)")
    lines.append("")

    # Per-family comparison
    lines.append("PER-FAMILY COMPARISON (top 15 by AILLA2 items)")
    lines.append("-" * 80)
    lines.append(f"{'Family':<35} {'API Items':>10} {'AILLA2 Items':>13} {'Change':>10}")
    lines.append("-" * 80)

    family_comp = comparison.groupby("language_family").agg(
        api_items=("api_total_items", lambda x: x.fillna(0).astype(int).sum()),
        ailla2_items=("ailla2_total_items", "sum"),
    ).sort_values("ailla2_items", ascending=False)

    for family, row in family_comp.head(15).iterrows():
        if not family or pd.isna(family):
            family = "(no family)"
        change = row["ailla2_items"] - row["api_items"]
        lines.append(
            f"{str(family):<35} {row['api_items']:>10} {row['ailla2_items']:>13} {change:>+10}"
        )
    lines.append("")

    # Languages with biggest changes
    comparison["change"] = comparison["ailla2_total_items"] - comparison["api_total_items"].fillna(0).astype(int)
    biggest_increases = comparison.nlargest(20, "change")

    lines.append("BIGGEST INCREASES (top 20)")
    lines.append("-" * 80)
    lines.append(f"{'Language':<30} {'Family':<20} {'API':>6} {'AILLA2':>8} {'Change':>8}")
    lines.append("-" * 80)

    for _, row in biggest_increases.iterrows():
        family = row["language_family"] if pd.notna(row["language_family"]) else ""
        lines.append(
            f"{str(row['name_en']):<30} {str(family):<20} "
            f"{int(row['api_total_items']) if pd.notna(row['api_total_items']) else 0:>6} "
            f"{row['ailla2_total_items']:>8} {row['change']:>+8}"
        )
    lines.append("")

    # Date range changes for languages with items
    lines.append("DATE RANGE CHANGES (languages where dates differ)")
    lines.append("-" * 80)
    lines.append(
        f"{'Language':<25} {'API Range':>15} {'AILLA2 Range':>15} {'Note':<20}"
    )
    lines.append("-" * 80)

    date_changes = comparison[
        (comparison["ailla2_total_items"] > 0) &
        (
            (comparison["api_earliest_year"] != comparison["ailla2_earliest_year"]) |
            (comparison["api_latest_year"] != comparison["ailla2_latest_year"])
        )
    ].sort_values("name_en")

    for _, row in date_changes.head(30).iterrows():
        api_e = int(row["api_earliest_year"]) if pd.notna(row["api_earliest_year"]) else "N/A"
        api_l = int(row["api_latest_year"]) if pd.notna(row["api_latest_year"]) else "N/A"
        a2_e = int(row["ailla2_earliest_year"]) if pd.notna(row["ailla2_earliest_year"]) else "N/A"
        a2_l = int(row["ailla2_latest_year"]) if pd.notna(row["ailla2_latest_year"]) else "N/A"
        note = ""
        if api_e == "N/A" and a2_e != "N/A":
            note = "NEW dates"
        elif api_e != "N/A" and a2_e != "N/A":
            note = "CHANGED"
        lines.append(
            f"{str(row['name_en']):<25} {str(api_e)+'-'+str(api_l):>15} "
            f"{str(a2_e)+'-'+str(a2_l):>15} {note:<20}"
        )
    lines.append("")

    # Mocho vs Tuzanteco check
    lines.append("MOCHO vs TUZANTECO VERIFICATION (ISO mhc)")
    lines.append("-" * 40)
    mhc = comparison[comparison["name_en"].isin(["Mocho", "Tuzanteco"])]
    for _, row in mhc.iterrows():
        lines.append(
            f"  {row['name_en']} (ID {row['language_id']}): "
            f"API={int(row['api_total_items']) if pd.notna(row['api_total_items']) else 0} items, "
            f"AILLA2={row['ailla2_total_items']} items"
        )
    lines.append("")

    # Deposit date statistics
    lines.append("DEPOSIT DATE STATISTICS (from Files sheet 'Date Uploaded')")
    lines.append("-" * 80)
    lines.append(f"Total files processed: {len(files)}")

    has_upload = files["Date Uploaded"].notna().sum() if "Date Uploaded" in files.columns else 0
    lines.append(f"Files with Date Uploaded: {has_upload}/{len(files)}")

    upload_years = files["Date Uploaded"].dropna().apply(parse_year)
    valid_years = upload_years.dropna()
    if len(valid_years) > 0:
        lines.append(f"Upload date range: {int(valid_years.min())}-{int(valid_years.max())}")
    lines.append("")

    has_deposit = comparison["earliest_deposit_year"].notna().sum()
    lines.append(f"Languages with deposit dates: {has_deposit}")
    lines.append("")

    # Earliest 20 languages by deposit date
    with_deposit = comparison[comparison["earliest_deposit_year"].notna()].copy()
    earliest_deposit = with_deposit.nsmallest(20, "earliest_deposit_year")

    lines.append("EARLIEST 20 LANGUAGES BY DEPOSIT DATE")
    lines.append("-" * 80)
    lines.append(
        f"{'Language':<30} {'Family':<20} {'Earliest Deposit':>16} {'Items':>6}"
    )
    lines.append("-" * 80)

    for _, row in earliest_deposit.iterrows():
        family = row["language_family"] if pd.notna(row["language_family"]) else ""
        dep = int(row["earliest_deposit_year"])
        items_count = row["ailla2_total_items"]
        lines.append(
            f"{str(row['name_en']):<30} {str(family):<20} {dep:>16} {items_count:>6}"
        )
    lines.append("")

    # Creation vs deposit date comparison
    both_dates = comparison[
        comparison["ailla2_earliest_year"].notna() &
        comparison["earliest_deposit_year"].notna()
    ].copy()
    both_dates["gap_years"] = (
        both_dates["earliest_deposit_year"] - both_dates["ailla2_earliest_year"]
    )

    lines.append("CREATION vs DEPOSIT DATE COMPARISON (top 20 by gap)")
    lines.append("-" * 80)
    lines.append(
        f"{'Language':<25} {'Created':>8} {'Deposited':>10} {'Gap (yrs)':>10}"
    )
    lines.append("-" * 80)

    biggest_gaps = both_dates.nlargest(20, "gap_years")
    for _, row in biggest_gaps.iterrows():
        created = int(row["ailla2_earliest_year"])
        deposited = int(row["earliest_deposit_year"])
        gap = int(row["gap_years"])
        lines.append(
            f"{str(row['name_en']):<25} {created:>8} {deposited:>10} {gap:>+10}"
        )
    lines.append("")

    if len(both_dates) > 0:
        lines.append(f"Average creation-to-deposit gap: {both_dates['gap_years'].mean():.1f} years")
        lines.append(f"Median creation-to-deposit gap: {both_dates['gap_years'].median():.1f} years")
        lines.append("")

    # Write report
    report_text = "\n".join(lines)
    REPORT_FILE.write_text(report_text, encoding="utf-8")
    print(f"\nReport saved to {REPORT_FILE}")

    # Also print to terminal
    print("\n" + report_text)


def main() -> None:
    """Main entry point: extract AILLA2 data and update languages CSV."""
    print("=" * 60)
    print("AILLA2 DATA EXTRACTION")
    print("=" * 60)

    # Step 1: Load all AILLA2 data (Items + Folders)
    items, folders = load_ailla2_data()

    # Step 2: Build folder -> language mapping
    folder_lang_map = build_folder_language_map(folders)

    # Step 3: Load Files sheets (needed for both item counting and deposit dates)
    files = load_files_data()

    # Step 4: Compute per-language item stats (hybrid: file-level + folder fallback)
    # Uses ALL items/files regardless of visibility
    lang_stats = compute_language_stats(items, files, folder_lang_map)

    # Step 5: Compute public-only item counts (PUB + LOG only)
    public_stats = compute_public_items(items, files, folder_lang_map)

    # Step 6: Compute deposit date stats (uses all files for date ranges)
    deposit_stats = compute_deposit_stats(files, folder_lang_map)

    # Step 7: Update languages_dataset.csv (items + public_items + deposit dates)
    languages, comparison = update_languages_csv(lang_stats, public_stats, deposit_stats)

    # Step 8: Generate verification report
    generate_report(comparison, items, folders, languages, files)

    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
