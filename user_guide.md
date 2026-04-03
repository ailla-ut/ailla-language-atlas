# AILLA Language Atlas: Staff User Guide

This guide covers how to create a new map for a different language family and how to maintain the existing maps. Adding a new language family is the most practical use of this pipeline, since item counts and date ranges are currently based on AILLA2 pre-migration data. Updating existing maps with new item data will become possible when a new data source (such as an AILLA3 data export) is available.

## Prerequisites

- Python 3.12+ (check with `python3 --version`)
- [uv](https://github.com/astral-sh/uv) package manager
- Google Cloud Translation API credentials (for trilingual translation)
- Access to the [ailla-ut/ailla-language-atlas](https://github.com/ailla-ut/ailla-language-atlas) GitHub repository (for pushing updated StoryMap files)
- WordPress login for sites.utexas.edu/ailla (for updating accessible HTML on the AILLA WordPress site)

Install dependencies:

```bash
uv sync
```

## Part 1: Creating a New Language Family Map

To create a map for a different language family (for example, Uto-Aztecan or Arawakan), follow these steps.

### Step 1: Choose a family

Run the family analysis to see which families have the deepest documentation:

```bash
uv run scripts/analyze_families.py
```

This outputs `data/family_analysis.csv` with all 54 families ranked by a weighted score based on temporal spread, geographic spread, holdings size, family size, and data quality. Look for families with high item counts, broad geographic coverage, and strong temporal data.

### Step 2: Add the family configuration

Open `scripts/build_storymaps.py` and add a new entry to the `FAMILIES` dictionary near the top of the file. Use the existing Mayan and Quechua entries as templates:

```python
"Arawakan": {
    "output_file": "arawakan_storymap.json",
    "preview_file": "preview_arawakan.html",
    "item_threshold": 10,  # adjust based on family size
    "title": "Arawakan Languages in the AILLA Archive",
    "subtitle": (
        "A chronological journey through the documentation of Arawakan "
        "languages in the Archive of the Indigenous Languages of Latin "
        "America (AILLA) at the University of Texas at Austin."
    ),
    "region_description": "South America and the Caribbean",
    "map_center": {"lat": -3.0, "lon": -60.0},
    "overview_zoom": 4,
},
```

The `item_threshold` determines how many items a language needs to get its own featured slide. Languages below the threshold appear in the closing summary slide.

### Step 3: Add geocoding coordinates

Open `scripts/geocode.py` and add a curated coordinates dictionary for the new family's languages. Each language needs a latitude and longitude targeting the actual language-speaking region (not the country centroid). Use resources like Ethnologue, or Wikipedia to find appropriate coordinates.

The script applies small random jitter to prevent pins from stacking when multiple languages are spoken in the same area.

### Step 4: Write curated descriptions

Add entries to `data/curated_descriptions.json` for each language that will get a featured slide. Each entry is keyed by `language_id` (not ISO code) and contains a narrative description. Follow the existing format:

- Open with 1-2 sentences of general context (speaker population, geographic location) drawn from Wikipedia and Ethnologue
- Focus on AILLA's holdings: collection names, contributor names, genres, item counts, date ranges
- Use the phrasing "with the earliest dedicated documentation from [year]"
- List native researchers and Indigenous communities before non-native researchers

Also add entries to the `_dedicated_doc_years` mapping at the top of the JSON file with the year dedicated documentation began for each featured language.

### Step 5: Build and translate

```bash
uv run scripts/build_storymaps.py
uv run scripts/translate_storymaps.py
uv run scripts/generate_wordpress_html.py
```

### Step 6: Create WordPress page

1. Create a new page in WordPress
2. Add a Custom HTML block with the iframe embed (use the GitHub Pages URL for the new preview HTML) followed by the accessible HTML content from `docs/wordpress_html/`
3. Use an explicit `</iframe>` closing tag (not self-closing), or content below the iframe will not render

## Part 2: Updating Existing Maps

Item counts and date ranges on the existing Mayan and Quechua maps are based on AILLA2 pre-migration data, since the AILLA3 API does not support full item extraction. Language and collection metadata (names, descriptions, country associations) can be refreshed from the API, but the holdings numbers shown on slides will not change until a new item data source is available.

When a new data source becomes available:

1. Adapt `scripts/extract_ailla2.py` to work with the new export format
2. Re-run the extraction to update `data/languages_dataset.csv`
3. Rebuild and retranslate the StoryMap JSONs
4. Regenerate the accessible WordPress HTML
5. Push to GitHub and update WordPress

To update only metadata (language names, descriptions, collection records):

```bash
uv run scripts/ailla_scraper.py --skip-items
uv run scripts/build_storymaps.py
uv run scripts/translate_storymaps.py
uv run scripts/generate_wordpress_html.py
```

Then push to GitHub and replace the HTML in each WordPress page's Custom HTML block with the new content from `docs/wordpress_html/`.

## Editing Curated Descriptions

To update the text of a curated description for an existing featured language, edit the entry in `data/curated_descriptions.json` and re-run `build_storymaps.py`, `translate_storymaps.py`, and `generate_wordpress_html.py`. The language_id key must match the `language_id` column in `languages_dataset.csv`.

## Common Issues

**Google Cloud Translation API errors:** Ensure your credentials are configured. The translation script requires a valid service account key.

**WordPress iframe not rendering content below it:** Make sure the iframe uses an explicit closing tag: `<iframe src="..."></iframe>`, not `<iframe src="..." />`. Self-closing iframe tags cause the browser to swallow all subsequent HTML.

**Item counts don't match AILLA website:** The pipeline uses AILLA2 data, which is a pre-migration snapshot. Items deposited after the migration to AILLA3 will not appear in the datasets. Manual overrides can be added in `scripts/extract_ailla2.py` for known discrepancies.