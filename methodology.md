# AILLA Language Atlas: Methodology

## Contents
- [Overview](#overview)
- [Data Sources](#data-sources)
- [Pipeline Stages](#pipeline-stages)
- [Editorial Decisions](#editorial-decisions)
- [Validation](#validation)
- [Notes](#notes)

## Overview

The AILLA Language Atlas is built through a six-stage data pipeline that extracts metadata from two sources, ranks language families by documentation depth, assigns geographic coordinates, generated interactive StoryMapJS visualizations, and translates them into three languages. The pipeline processes metadata for 641 languages across 263 archival collections, ultimately producing a pair of trilingual interactive maps covering the Mayan and Quechua language families.

This document describes the technical methodology, editorial decisions, and validation procedures used throughout the project. AI tools (Claude Code) were used for pipeline development, data analysis, and StoryMap construction. The Google Cloud Translation API was used for translating slide content and narrative analysis text into Spanish and Portuguese.

## Data Sources

The atlas draws on two complementary data sources. Neither is complete on its own, and using both together provides a more accurate picture of AILLA’s holdings than either source alone.

### AILLA API

AILLA’s API provides structured metadata that the pipeline extracts in three categories: languages (641 records), collections (263 records), and countries (64 records). These provide language names in English, Spanish, and Portuguese, ISO 639-3 codes, language family classifications, country associations, collection metadata, and language descriptions. All data is publicly accessible metadata.

The API also lists individual items, but this data was not used for item counts or date ranges due to frequent server errors during extraction. Item-level data comes from the AILLA2 spreadsheets instead.

### AILLA2 Spreadsheets

AILLA2 is the second version of AILLA’s digital repository system. The pipeline draws on 7 Excel workbooks exported from AILLA2 prior to the system’s migration, containing 18,548 items and 118,893 files across Items, Folders, and Files sheets. These spreadsheets are the source of truth for item counts, creation dates, deposit dates, and per-file language attribution. They are not publicly distributed.

The two sources complement each other; the API provides language metadata and descriptions, while AILLA2 provides the item counts, date ranges, and deposit dates.

## Pipeline Stages

### 1. API Extraction (`scripts/ailla_scraper.py`)

The scraper extracts language, collection, and country metadata from AILLA's API with rate limiting to avoid overloading the server. Raw responses are saved for reproducibility.

The extracted data is reorganized into CSV tables with separate columns for each language (English, Spanish, Portuguese). Language records are then linked to their associated collections.

### AILLA2 Extraction (`scripts/extract_ailla2.py`)

This is the most complex stage. It reads all 7 Excel workbooks and computes per-language statistics: total item counts, public item counts, creation date ranges, and deposit date ranges.

AILLA2 tracks langauge associations at two levels: individual files can be tagged with specific languages, and folders (which group related files) can be tagged with broader language lists. The extraction uses both levels to count items per language. When file-level tags are available, the script prefers them the more precise source. When. folder's language tags include langauges not covered by any of its files' tags (common with comparative works that span multiple languages), the script supplements from the folder level. For the small number of folders with no file-level data at all, the script falls back to folder-level tags entirely. Languages used for description or translation (English, Spanish, Portugeues) are excluded from counts so that a recording described in English is not counted as an English-language item.

Creation dates come from the Items sheet and deposit dates from the Files sheet.

Several manual overrides correct known issues in the source data:
- **Yauyos Quechua:** Files in two unrelated folders are incorrectly tagged with this language, pulling in dates from other collections. Overriden to the actual collection dates (2001-2014)
- **Ancash Huaylas Quechua:** The spreadsheets capture only 5 items, but the live AILLA site shows 33 (deposited after the AILLA 2 export)
- **Ixil and Inga:** Missing country data in the API. Overriden to Guatemala and Colombia respectively.

### 3. Family Analysis (`scripts/analyze_families.py`)

This stage ranks al 54 language families by weighted composite score to determine which families warrant their own maps. The scoring weights five factors: temporal spread (25%), geographic spread (20%), holdings size (20%), family size (20%), and data quality (15%).

Mayan ranked fourth and Quechua ranked sixth. The two families were selected for their documentation depth and geographic contrast: Mayan concentrates in Mesoamerica while Quechua spans the Andes.

### 4. Geocoding (`scripts/geocode.py`)

The geocoding stage assigns curated latitude/longitude coordinates to the 33 Mayan and 17 Quechua languages. Coordinates target actual language-speaking regions rather than country centroids. A small random jitter is applied to prevent pins from stacking on top of each other when multiple languages are spoken in the same area.

### 5. StoryMapJS Generation (`scripts/build_storymaps.py`)

This stage filters geocoded languages by family, applies item thresholds (100+ items for Mayan, 5+ for Quechua), and generates StoryMapJS JSON files with thre types of slides:

- **Title/overview slide:** Introduces the map with a description and total language count.
- **Individual language slides:** One per featured language, sorted chronologically by dedicated documentation start date. Each slide includes a curated description, metadata (countries, holdings, dates, ISO code), and a link to the language's AILLA page.
- **Summary slide:** Lists languages below the threshold and languages excluded due to high restriction percentages.

Curated descriptions are loaded from `data/curated_descriptions.json`. When no curated description exists, the script falls back to the language description provided by AILLA 

### 6. Translation (`scripts/translate_storymaps.py`)

The translation stage uses the Google Cloud Translation API to produce Spanish and Portuguese versions of each StoryMap. A curated lookup dictionary handles standardized phrases (metadata labels, UI text) to avoid machine-translating terms that should stay consistent. Curated descriptions and the overview/summary slides are translated via the API, while metadata values (language names, ISO codes, dates) pass through unchanged.

## Editorial Decisions

### Dedicated Documentation Dates

The atlas sorts slides by the start of "dedicated documentation" rather than by the raw `earliest_item_year` from the CSV. The earliest item date for a language may come from a document that predates sustained fieldwork on that language, so the atlas uses curated dates that reflect when substantial language-specific documentation began.

"Dedicated documentation" is defined as 5 or more items per year focused on a single language, or the beginning of a consecutive multi-year documentation effort. These curated dates are stored in the `_dedicated_doc_years` mapping within `curated_descriptions.json` and were verified against temporal distribution analysis of every featured language.

Slides use the phrasing "with the earliest dedicated documentation from [year]" (start date only, no range) to avoid implying continuous documentation. The metadata field "Dates created" still shows the full range from the CSV.

### Approximate Dates

When source dates include only a year with zeroed month and day fields (e.g., "1960-00-00"), descriptions use the decade ("the 1960s") rather than a specific year to avoid overstating precision.

### Restrictions and Access

Approximately 25% of items in the Mayan family and 22% in the Quechua family are restricted or embargoed. The atlas handles this through a dual-threshold system: a language is featured if its public items meet the threshold, OR if its total items meet the threshold and less than 50% are restricted. Langauges where more than half of the items are restricted are listed in the summary slide rather than receiving their own featured slide. Featured languages with significant restricted holdings include a note in their slide text indicating how many items are publicly accessible.

### Researcher Attribution

Curated descriptions list native researchers and Indigenous communities before American and European researchers. Only dedicated or primarily-focused collections are mentioned by name; administrative collections (AILLA Collection Guides) and conference proceedings (CILLA) are omitted even if they contain items in the featured language.

### Description Sources

Curated descriptions draw on two approved external sources for opening context (speaker populations, geographic location, linguistic classification): Wikipedia and Ethnologue (approved by Dr. Kung). All remaining content comes from AILLA-verifiable data: collection names, contributor names, genre distributions, item counts, and date ranges. Specific place names from collection descriptions are not included unless independently verified, since they may reflect a single researcher's fieldwork site rather than the full geographic range of the language.

## Validation

### Data Verification

An independent verification script re-derived all date fields from the AILLA2 source data and compared them against the CSV. Results: 548 of 548 creation dates matched, 542 of 542 deposit dates matched, and all 28 dedicated documentation years matched their description text.

### API vs. AILLA2 Comprarison

The extraction pipeline compares item counts between the API and the AILLA2 spreadsheets. Because many items are associated with more than one language (for example, a comparative work tagged with several languages), the per-language totals are higher than the raw item count. The AILLA2 extraction captures more of these associations than the API alone, particularly for items where folder-level language tags are broader than the file-level tags.

### Spot-Checking

Individual languages were periodically checked against the live AILLA website during development, which led to the discovery of the Ancash Huayllas Quechua item count discrepancy noted above.

## Notes

The pipeline accesses only publicly available API metadata and respects AILLA's access restrictions. The datasets represent a point-in-time extraction; the pipeline can be re-run to capture new deposits. All datasets and text are available on GitHub (github.com/ailla-ut/ailla-language-atlas) under a CC BY-NC-SA 4.0 license and will be deposited at Texas ScholarWorks (repositories.lib.utexas.edu).

