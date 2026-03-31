# AILLA Language Atlas: Methodology

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
-**Ancash Huaylas Quechua:** The spreadsheets capture only 5 items, but the live AILLA site shows 33 (deposited after the AILLA 2 export)
-**Ixil and Inga:** Missing country data in the API. Overriden to Guatemala and Colombia respectively.

