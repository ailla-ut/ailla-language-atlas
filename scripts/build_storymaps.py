"""Generate StoryMapJS JSON files for the AILLA Language Atlas.

Builds two interactive StoryMapJS maps from the geocoded languages dataset:
1. Mayan Language Family (Mesoamerica) - languages with 100+ items as individual slides
2. Quechua Language Family (Andes) - languages with 5+ items as individual slides

Each map includes:
- A title/overview slide showing all language locations
- Individual language slides sorted chronologically by start of dedicated documentation
- A closing summary slide listing languages below the item threshold

Usage:
    uv run scripts/build_storymaps.py

Output:
    data/mayan_storymap.json
    data/quechua_storymap.json
"""

import json
import math
import pandas as pd
from pathlib import Path
from typing import Any


# --- Configuration ---

FAMILIES: dict[str, dict[str, Any]] = {
    "Mayan": {
        "output_file": "mayan_storymap.json",
        "preview_file": "preview_mayan.html",
        "item_threshold": 100,
        "title": "Mayan Languages in the AILLA Archive",
        "subtitle": (
            "A chronological journey through the documentation of Mayan languages "
            "in the Archive of the Indigenous Languages of Latin America (AILLA) "
            "at the University of Texas at Austin."
        ),
        "region_description": "Mesoamerica (Guatemala, Belize, Mexico)",
        "map_center": {"lat": 16.0, "lon": -90.5},
        "overview_zoom": 5,
    },
    "Quechua": {
        "output_file": "quechua_storymap.json",
        "preview_file": "preview_quechua.html",
        "item_threshold": 5,
        "title": "Quechua Languages in the AILLA Archive",
        "subtitle": (
            "A chronological journey through the documentation of Quechua languages "
            "in the Archive of the Indigenous Languages of Latin America (AILLA) "
            "at the University of Texas at Austin."
        ),
        "region_description": "the Andes (Argentina, Bolivia, Colombia, Ecuador, Peru)",
        "map_center": {"lat": -8.0, "lon": -75.0},
        "overview_zoom": 4,
    },
}

AILLA_BASE_URL = "https://ailla.utexas.org"

# Languages with 0 items in AILLA2 but represented through multi-language
# collections. These aren't individually tagged at the item level in the
# spreadsheets, but materials exist within broader collections.
# Each entry: (language_id, name, language_url, collection_name, collection_url)
COLLECTION_ONLY_LANGUAGES: list[tuple[int, str, str, str, str]] = [
    (476, "Ch'olti'", f"{AILLA_BASE_URL}/languages/476",
     "Mayan Languages Collection of Terrence Kaufman",
     f"{AILLA_BASE_URL}/collections/783"),
]

# Languages where 50%+ of items are restricted are excluded from featured slides
# but mentioned in the summary slide (their metadata is visible on AILLA, but
# the holdings are predominantly access-controlled)
RESTRICTED_THRESHOLD = 0.50

# Curated narrative descriptions loaded from data/curated_descriptions.json,
# keyed by language_id (not ISO code, since Mocho and Tuzanteco share ISO mhc).
# Each entry is reviewed and approved before inclusion. Falls back to AILLA's
# staff-authored description field when no curated description exists.
_DESCRIPTIONS_PATH = Path(__file__).parent.parent / "data" / "curated_descriptions.json"

def _load_curated_descriptions() -> tuple[dict[int, str], dict[int, int]]:
    """Load curated descriptions and dedicated doc years from JSON file.

    Returns:
        Tuple of (descriptions dict, dedicated doc years dict), both keyed by
        language_id as int.
    """
    if not _DESCRIPTIONS_PATH.exists():
        return {}, {}
    with open(_DESCRIPTIONS_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    descriptions = {int(k): v for k, v in raw.items()
                    if k != "_comment" and k != "_dedicated_doc_years"}
    doc_years = {int(k): v for k, v in raw.get("_dedicated_doc_years", {}).items()}
    return descriptions, doc_years

CURATED_DESCRIPTIONS: dict[int, str]
DEDICATED_DOC_YEARS: dict[int, int]
CURATED_DESCRIPTIONS, DEDICATED_DOC_YEARS = _load_curated_descriptions()


def build_slide_text(row: pd.Series) -> str:
    """Generate HTML description text for a language slide.

    Combines available metadata into a readable slide description including
    sociolinguistic context, AILLA holdings summary, and a direct link.

    Args:
        row: A row from the languages DataFrame.

    Returns:
        HTML-formatted slide text.
    """
    parts = []

    # Language description: prefer curated description, fall back to AILLA staff description
    lang_id = row.get("language_id")
    curated = CURATED_DESCRIPTIONS.get(int(lang_id)) if pd.notna(lang_id) else None
    if curated:
        parts.append(f"<p>{curated}</p>")
    else:
        desc = row.get("description", "")
        if pd.notna(desc) and str(desc).strip():
            desc_text = str(desc).strip()
            if len(desc_text) > 400:
                desc_text = desc_text[:397] + "..."
            parts.append(f"<p>{desc_text}</p>")

    # Indigenous name
    indigenous = row.get("indigenous_name", "")
    if pd.notna(indigenous) and str(indigenous).strip():
        parts.append(f"<p><strong>Indigenous name:</strong> {indigenous}</p>")

    # Alternative names
    alt_names = row.get("alternative_name", "")
    if pd.notna(alt_names) and str(alt_names).strip():
        parts.append(f"<p><strong>Also known as:</strong> {alt_names}</p>")

    # Countries
    countries = row.get("countries", "")
    if pd.notna(countries) and str(countries).strip():
        parts.append(f"<p><strong>Countries:</strong> {countries}</p>")

    # Holdings summary
    total = int(row["total_items"]) if pd.notna(row["total_items"]) else 0
    public = int(row["public_items"]) if pd.notna(row.get("public_items")) else total
    collections = int(row["collection_count"]) if pd.notna(row["collection_count"]) else 0

    holdings_parts = []
    if total > 0:
        item_text = f"{total} item{'s' if total != 1 else ''}"
        if public < total:
            item_text += f" ({public} publicly accessible)"
        holdings_parts.append(item_text)
    if collections > 0:
        holdings_parts.append(
            f"{collections} collection{'s' if collections != 1 else ''}"
        )

    if holdings_parts:
        parts.append(f"<p><strong>AILLA holdings:</strong> {', '.join(holdings_parts)}</p>")

    # Dates created (from item creation dates)
    earliest = row.get("earliest_item_year")
    latest = row.get("latest_item_year")
    if pd.notna(earliest):
        earliest_int = int(earliest)
        latest_int = int(latest) if pd.notna(latest) else earliest_int
        if earliest_int == latest_int:
            parts.append(f"<p><strong>Dates created:</strong> {earliest_int}</p>")
        else:
            parts.append(
                f"<p><strong>Dates created:</strong> {earliest_int}-{latest_int}</p>"
            )

    # Dates deposited (from file upload dates)
    dep_earliest = row.get("earliest_deposit_year")
    dep_latest = row.get("latest_deposit_year")
    if pd.notna(dep_earliest):
        dep_earliest_int = int(dep_earliest)
        dep_latest_int = int(dep_latest) if pd.notna(dep_latest) else dep_earliest_int
        if dep_earliest_int == dep_latest_int:
            parts.append(f"<p><strong>Dates deposited:</strong> {dep_earliest_int}</p>")
        else:
            parts.append(
                f"<p><strong>Dates deposited:</strong> {dep_earliest_int}-{dep_latest_int}</p>"
            )

    # ISO code
    iso = row.get("iso_639_3_code", "")
    if pd.notna(iso) and str(iso).strip():
        parts.append(f"<p><strong>ISO 639-3:</strong> {iso}</p>")

    # AILLA link
    url = row.get("ailla_language_url", "")
    if pd.notna(url) and str(url).strip():
        parts.append(
            f'<p><a href="{url}" target="_blank">View in AILLA</a></p>'
        )

    return "\n".join(parts)


def build_headline(row: pd.Series) -> str:
    """Generate the slide headline for a language.

    Args:
        row: A row from the languages DataFrame.

    Returns:
        Headline string (language name only; dates appear in metadata fields).
    """
    return row["name_en"]


def build_title_slide(config: dict[str, Any], slide_count: int,
                      summary_count: int) -> dict[str, Any]:
    """Build the overview/title slide for a StoryMap.

    The title slide uses type "overview" which shows all subsequent locations.

    Args:
        config: Family configuration dict.
        slide_count: Number of individual language slides.
        summary_count: Number of languages in the closing summary.

    Returns:
        StoryMapJS slide dict.
    """
    total = slide_count + summary_count
    text = (
        f"<p>{config['subtitle']}</p>"
        f"<p>This map features <strong>{slide_count} languages</strong> with "
        f"detailed slides, covering {config['region_description']}. "
    )
    if summary_count > 0:
        text += (
            f"An additional {summary_count} language{'s' if summary_count != 1 else ''} "
            f"{'are' if summary_count != 1 else 'is'} listed in the closing summary."
        )
    text += (
        f"</p><p>Slides are ordered chronologically by the start of dedicated "
        f"documentation for each language in AILLA's holdings.</p>"
        f'<p><a href="{AILLA_BASE_URL}" target="_blank">Visit AILLA</a></p>'
    )

    return {
        "type": "overview",
        "text": {
            "headline": config["title"],
            "text": text,
        },
        "location": {
            "lat": config["map_center"]["lat"],
            "lon": config["map_center"]["lon"],
        },
    }


def build_language_slide(row: pd.Series) -> dict[str, Any]:
    """Build an individual language slide.

    Args:
        row: A row from the geocoded languages DataFrame.

    Returns:
        StoryMapJS slide dict.
    """
    slide: dict[str, Any] = {
        "text": {
            "headline": build_headline(row),
            "text": build_slide_text(row),
        },
        "location": {
            "lat": float(row["latitude"]),
            "lon": float(row["longitude"]),
            "line": True,
        },
    }

    return slide


def _format_summary_entry(row: pd.Series) -> str:
    """Format a single language entry for the summary slide.

    Shows total items with public count when they differ.

    Args:
        row: A row from the languages DataFrame.

    Returns:
        HTML string for the language entry.
    """
    name = row["name_en"]
    total = int(row["total_items"]) if pd.notna(row["total_items"]) else 0
    public = int(row["public_items"]) if pd.notna(row.get("public_items")) else total
    url = row.get("ailla_language_url", "")

    count_text = f"{total} item{'s' if total != 1 else ''}"
    if public < total:
        count_text += f", {public} public"

    if pd.notna(url) and str(url).strip():
        return f'<a href="{url}" target="_blank">{name}</a> ({count_text})'
    return f"{name} ({count_text})"


def build_summary_slide(below_threshold: pd.DataFrame,
                        restricted: pd.DataFrame,
                        config: dict[str, Any],
                        family_name: str,
                        collection_only: list[tuple[int, str, str, str, str]] | None = None) -> dict[str, Any]:
    """Build the closing summary slide for non-featured languages.

    Separates languages into three groups:
    1. Restricted holdings: meet the item threshold but 50%+ restricted
    2. Below threshold: fewer items than required for a featured slide
    3. Collection-only: 0 items in AILLA2 but represented through multi-language collections

    Args:
        below_threshold: DataFrame of languages below the item threshold.
        restricted: DataFrame of languages excluded due to high restricted %.
        config: Family configuration dict.
        family_name: Name of the language family.
        collection_only: List of (language_id, name, lang_url, collection_name, collection_url) tuples.

    Returns:
        StoryMapJS slide dict.
    """
    threshold = config["item_threshold"]
    sections = []

    # Section 1: Languages with restricted holdings (if any)
    if len(restricted) > 0:
        restricted_list = []
        for _, row in restricted.sort_values("name_en").iterrows():
            restricted_list.append(_format_summary_entry(row))

        sections.append(
            f"<p>The following {'language has' if len(restricted) == 1 else f'{len(restricted)} languages have'} "
            f"significant holdings in AILLA, but public materials account for "
            f"fewer than {threshold} items:</p>"
            f"<p>{'<br>'.join(restricted_list)}</p>"
        )

    # Section 2: Languages below threshold
    if len(below_threshold) > 0:
        lang_list = []
        for _, row in below_threshold.sort_values("name_en").iterrows():
            lang_list.append(_format_summary_entry(row))

        sections.append(
            f"<p>The following {len(below_threshold)} languages in the "
            f"{family_name} family "
            f"have fewer than {threshold} items in AILLA's current holdings. "
            f"As AILLA's collections grow, these languages may be featured in future "
            f"updates to this map.</p>"
            f"<p>{'<br>'.join(lang_list)}</p>"
        )

    # Section 3: Languages represented only through multi-language collections
    if collection_only is None:
        collection_only = []
    if collection_only:
        col_list = []
        for _lid, name, lang_url, col_name, col_url in sorted(collection_only, key=lambda x: x[1]):
            col_list.append(
                f'<a href="{lang_url}" target="_blank">{name}</a> '
                f'(materials in the <a href="{col_url}" target="_blank">{col_name}</a>)'
            )
        sections.append(
            f"<p>The following {'language is' if len(collection_only) == 1 else f'{len(collection_only)} languages are'} "
            f"represented in AILLA through multi-language collections:</p>"
            f"<p>{'<br>'.join(col_list)}</p>"
        )

    sections.append(
        f'<p><a href="{AILLA_BASE_URL}" target="_blank">Explore all AILLA collections</a></p>'
    )

    return {
        "text": {
            "headline": "Additional Languages",
            "text": "\n".join(sections),
        },
        "location": {
            "lat": config["map_center"]["lat"],
            "lon": config["map_center"]["lon"],
        },
    }


def build_storymap(df: pd.DataFrame, family_name: str,
                   config: dict[str, Any]) -> dict[str, Any]:
    """Build a complete StoryMapJS JSON structure for a language family.

    Filters languages by family, separates them into featured slides (above
    item threshold) and summary languages (below threshold), sorts featured
    languages chronologically, and assembles the full StoryMapJS structure.

    Args:
        df: Full languages DataFrame with geocoding.
        family_name: Name of the language family to filter by.
        config: Family configuration dict.

    Returns:
        Complete StoryMapJS JSON structure ready for serialization.
    """
    threshold = config["item_threshold"]

    # Filter to this family with valid coordinates
    family_df = df[
        (df["language_family"] == family_name) & df["latitude"].notna()
    ].copy()

    print(f"\n{'='*60}")
    print(f"Building {family_name} StoryMap")
    print(f"{'='*60}")
    print(f"Total languages in family: {len(family_df)}")

    # Compute restricted percentage for each language
    if "public_items" in family_df.columns:
        family_df["restricted_pct"] = family_df.apply(
            lambda r: (r["total_items"] - r["public_items"]) / r["total_items"]
            if r["total_items"] > 0 else 0.0,
            axis=1,
        )
    else:
        family_df["restricted_pct"] = 0.0

    # A language is featured if EITHER:
    # 1. public_items >= threshold (qualifies on public holdings alone)
    # 2. total_items >= threshold AND restricted_pct < RESTRICTED_THRESHOLD
    public_col = "public_items" if "public_items" in family_df.columns else "total_items"
    qualifies_public = family_df[public_col] >= threshold
    qualifies_total = (family_df["total_items"] >= threshold) & (family_df["restricted_pct"] < RESTRICTED_THRESHOLD)

    featured = family_df[qualifies_public | qualifies_total].copy()

    # Languages excluded due to high restricted %: meet total threshold but not featured
    restricted_excluded = family_df[
        (family_df["total_items"] >= threshold) &
        ~(qualifies_public | qualifies_total)
    ].copy()

    # Languages below threshold entirely (exclude 0-item languages)
    below = family_df[
        (family_df["total_items"] < threshold) &
        (family_df["total_items"] > 0) &
        ~(qualifies_public)
    ].copy()

    print(f"Featured slides (>= {threshold} items): {len(featured)}")
    print(f"Restricted holdings (excluded): {len(restricted_excluded)}")
    print(f"Below threshold: {len(below)}")

    if len(restricted_excluded) > 0:
        for _, row in restricted_excluded.iterrows():
            print(f"  Restricted: {row['name_en']} ({int(row['total_items'])} total, "
                  f"{int(row[public_col])} public, {row['restricted_pct']:.0%} restricted)")

    # Sort featured languages chronologically by dedicated documentation date
    # (curated dates that exclude comparative works); fall back to earliest_item_year
    featured["dedicated_doc_year"] = featured["language_id"].map(
        lambda lid: DEDICATED_DOC_YEARS.get(int(lid))
    ).fillna(featured["earliest_item_year"])
    featured = featured.sort_values(
        ["dedicated_doc_year", "name_en"],
        na_position="last",
    )

    # Count languages represented only through multi-language collections
    collection_only = [
        entry for entry in COLLECTION_ONLY_LANGUAGES
        if entry[0] in family_df["language_id"].values
    ]
    if collection_only:
        print(f"Collection-only: {len(collection_only)}")

    # Build slides
    slides = []

    # 1. Title/overview slide
    summary_count = len(below) + len(restricted_excluded) + len(collection_only)
    title_slide = build_title_slide(config, len(featured), summary_count)
    slides.append(title_slide)

    # 2. Individual language slides
    for _, row in featured.iterrows():
        slide = build_language_slide(row)
        slides.append(slide)
        print(f"  Slide: {build_headline(row)}")

    # 3. Closing summary slide (if there are non-featured languages)
    if len(below) > 0 or len(restricted_excluded) > 0 or len(collection_only) > 0:
        summary = build_summary_slide(below, restricted_excluded, config, family_name, collection_only)
        slides.append(summary)
        print(f"  Summary slide: {len(below)} below threshold, "
              f"{len(restricted_excluded)} restricted, "
              f"{len(collection_only)} collection-only")

    # Assemble StoryMapJS structure
    storymap = {
        "storymap": {
            "language": "en",
            "map_type": "stamen:terrain",
            "slides": slides,
            "calculate_zoom": True,
        },
    }

    print(f"Total slides: {len(slides)}")
    return storymap


def validate_storymap(data: dict[str, Any], name: str) -> bool:
    """Validate a StoryMapJS JSON structure.

    Checks for required fields and data integrity.

    Args:
        data: StoryMapJS JSON dict.
        name: Name for error reporting.

    Returns:
        True if valid, False otherwise.
    """
    errors = []

    if "storymap" not in data:
        errors.append("Missing root 'storymap' key")
        print(f"VALIDATION FAILED for {name}: {errors}")
        return False

    sm = data["storymap"]
    if "slides" not in sm:
        errors.append("Missing 'slides' array")

    slides = sm.get("slides", [])
    if len(slides) < 2:
        errors.append(f"Too few slides ({len(slides)}), expected at least 2")

    # Check title slide
    if slides and slides[0].get("type") != "overview":
        errors.append("First slide should be type 'overview'")

    # Check language slides have required fields
    for i, slide in enumerate(slides[1:], start=1):
        if "location" not in slide:
            errors.append(f"Slide {i} missing 'location'")
        else:
            loc = slide["location"]
            if "lat" not in loc or "lon" not in loc:
                errors.append(f"Slide {i} missing lat/lon")
            elif not isinstance(loc["lat"], (int, float)) or math.isnan(loc["lat"]):
                errors.append(f"Slide {i} has invalid latitude: {loc['lat']}")
            elif not isinstance(loc["lon"], (int, float)) or math.isnan(loc["lon"]):
                errors.append(f"Slide {i} has invalid longitude: {loc['lon']}")

        if "text" not in slide:
            errors.append(f"Slide {i} missing 'text'")
        elif "headline" not in slide.get("text", {}):
            errors.append(f"Slide {i} missing headline")

    if errors:
        print(f"\nVALIDATION ERRORS for {name}:")
        for e in errors:
            print(f"  - {e}")
        return False

    print(f"\nValidation passed for {name}: {len(slides)} slides OK")
    return True


def generate_preview_html(storymap_data: dict[str, Any], title: str,
                          output_path: Path) -> None:
    """Generate a self-contained HTML preview file for a StoryMap.

    Embeds the StoryMap JSON inline so the preview works by opening
    the file directly in a browser (no server needed).

    Args:
        storymap_data: Complete StoryMapJS JSON structure.
        title: Title for the HTML page.
        output_path: Path to write the HTML file.
    """
    json_str = json.dumps(storymap_data, ensure_ascii=False)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Preview: {title}</title>
    <link rel="stylesheet" href="https://cdn.knightlab.com/libs/storymapjs/latest/css/storymap.css">
    <style>body {{ margin: 0; padding: 0; }} #storymap {{ width: 100%; height: 100vh; }}</style>
</head>
<body>
    <div id="storymap"></div>
    <script src="https://cdn.knightlab.com/libs/storymapjs/latest/js/storymap-min.js"></script>
    <script>
        var storymap_data = {json_str};
        var storymap = new KLStoryMap.StoryMap("storymap", storymap_data, {{}});
        window.addEventListener("resize", function() {{ storymap.updateDisplay(); }});
    </script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved preview: {output_path}")


def main() -> None:
    """Build StoryMapJS JSON files for Mayan and Quechua families."""
    data_dir = Path(__file__).parent.parent / "data"
    csv_path = data_dir / "languages_dataset.csv"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found. Run ailla_scraper.py first.")
        return

    print("Building AILLA Language Atlas StoryMaps")
    print(f"Reading {csv_path}")
    df = pd.read_csv(csv_path)

    # Check for geocoding
    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("Error: latitude/longitude columns not found. Run geocode.py first.")
        return

    project_root = Path(__file__).parent.parent

    for family_name, config in FAMILIES.items():
        storymap = build_storymap(df, family_name, config)

        # Validate
        valid = validate_storymap(storymap, family_name)
        if not valid:
            print(f"WARNING: {family_name} StoryMap has validation errors.")

        # Write JSON
        output_path = data_dir / config["output_file"]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(storymap, f, indent=2, ensure_ascii=False)
        print(f"Saved: {output_path}")

        # Write preview HTML
        preview_path = project_root / config["preview_file"]
        generate_preview_html(storymap, config["title"], preview_path)

    print("\nDone. Open preview_mayan.html or preview_quechua.html in a browser.")


if __name__ == "__main__":
    main()
