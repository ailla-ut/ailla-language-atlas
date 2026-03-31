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

This is the most complex stage. It reasds all 7 Excel workbooks 