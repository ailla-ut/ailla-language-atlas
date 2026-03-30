#!/usr/bin/env python3
"""
Language Family Analysis for StoryMapJS Selection
===================================================

Analyzes AILLA language families to identify the best 2-3 candidates for
interactive StoryMapJS visualizations. Ranks families by:

1. Temporal spread: Items spanning many years (storytelling potential)
2. Geographic spread: Languages across multiple countries (map interest)
3. Rich AILLA holdings: Enough collections/items per language (slide content)
4. Right size: 10-30 languages (fills a map without overwhelming)
5. Data quality: Languages with names, descriptions, ISO codes

Usage:
    uv run scripts/analyze_families.py

Output:
    - Terminal: Ranked summary + top-10 shortlist with commentary
    - data/family_analysis.csv: Full rankings for all families

Author: LBDS Fellow, Benson Latin American Collection
Date: 2026-02-23
"""

import pandas as pd
import sys
from pathlib import Path

# Paths
DATA_DIR = Path("data")
LANGUAGES_FILE = DATA_DIR / "languages_dataset.csv"
ITEMS_FILE = DATA_DIR / "items_dataset.csv"
COLLECTIONS_FILE = DATA_DIR / "collections_dataset.csv"
OUTPUT_FILE = DATA_DIR / "family_analysis.csv"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load and validate the three input datasets."""
    print("Loading datasets...")

    if not LANGUAGES_FILE.exists():
        print(f"ERROR: {LANGUAGES_FILE} not found. Run the scraper first.")
        sys.exit(1)

    languages = pd.read_csv(LANGUAGES_FILE)
    print(f"  Languages: {len(languages)} records")

    # Check for temporal fields (indicates items extraction was run)
    if "earliest_item_year" not in languages.columns:
        print("ERROR: languages_dataset.csv is missing temporal fields.")
        print("  Run the full scraper first: uv run scripts/ailla_scraper.py")
        sys.exit(1)

    items = pd.read_csv(ITEMS_FILE) if ITEMS_FILE.exists() else pd.DataFrame()
    print(f"  Items: {len(items)} records")

    collections = pd.read_csv(COLLECTIONS_FILE) if COLLECTIONS_FILE.exists() else pd.DataFrame()
    print(f"  Collections: {len(collections)} records")

    return languages, items, collections


def analyze_families(languages: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """
    Compute metrics for each language family.

    For each of the 54 families, calculates temporal spread, geographic spread,
    documentation richness, size, and data quality indicators.
    """
    print("\nAnalyzing language families...")

    # Filter to languages that have a family classification
    langs_with_family = languages[languages["language_family"] != ""].copy()
    langs_no_family = len(languages) - len(langs_with_family)
    print(f"  Languages with family classification: {len(langs_with_family)}")
    print(f"  Languages without family (excluded): {langs_no_family}")

    # Convert temporal fields: coerce to numeric, NaN for empty strings
    langs_with_family["earliest_year_num"] = pd.to_numeric(
        langs_with_family["earliest_item_year"], errors="coerce"
    )
    langs_with_family["latest_year_num"] = pd.to_numeric(
        langs_with_family["latest_item_year"], errors="coerce"
    )
    langs_with_family["total_items_num"] = pd.to_numeric(
        langs_with_family["total_items"], errors="coerce"
    ).fillna(0).astype(int)

    results = []

    for family_name, group in langs_with_family.groupby("language_family"):
        # Size metrics
        num_languages = len(group)
        num_with_items = (group["total_items_num"] > 0).sum()
        num_with_collections = (group["collection_count"] > 0).sum()

        # Temporal metrics (from language-level rollups)
        valid_earliest = group["earliest_year_num"].dropna()
        valid_latest = group["latest_year_num"].dropna()

        earliest_year = int(valid_earliest.min()) if len(valid_earliest) > 0 else None
        latest_year = int(valid_latest.max()) if len(valid_latest) > 0 else None
        year_span = (latest_year - earliest_year) if earliest_year and latest_year else 0

        # Count languages with date data
        num_with_dates = len(valid_earliest)

        # Holdings metrics
        total_items = int(group["total_items_num"].sum())
        total_collections = int(group["collection_count"].sum())
        avg_items_per_lang = total_items / num_languages if num_languages > 0 else 0
        avg_collections_per_lang = total_collections / num_languages if num_languages > 0 else 0

        # Geographic metrics
        all_countries = set()
        for countries_str in group["countries"].dropna():
            if countries_str and str(countries_str).strip():
                for c in str(countries_str).split(";"):
                    c = c.strip()
                    if c:
                        all_countries.add(c)
        num_countries = len(all_countries)
        countries_list = "; ".join(sorted(all_countries))

        # Data quality metrics
        num_with_iso = (group["iso_639_3_code"].notna() & (group["iso_639_3_code"] != "")).sum()
        num_with_description = (group["description"].notna() & (group["description"] != "")).sum()
        num_with_indigenous_name = (group["indigenous_name"].notna() & (group["indigenous_name"] != "")).sum()

        # Percentage of items with valid dates (from items dataset)
        pct_with_dates = (num_with_dates / num_with_items * 100) if num_with_items > 0 else 0

        # Family ID (take first one found)
        family_id = group["language_family_id"].iloc[0] if len(group) > 0 else ""

        results.append({
            "family_name": family_name,
            "family_id": family_id,
            "num_languages": num_languages,
            "num_languages_with_items": int(num_with_items),
            "num_languages_with_collections": int(num_with_collections),
            "total_items": total_items,
            "total_collections": total_collections,
            "earliest_year": earliest_year if earliest_year else "",
            "latest_year": latest_year if latest_year else "",
            "year_span": year_span,
            "num_languages_with_dates": int(num_with_dates),
            "num_countries": num_countries,
            "countries_list": countries_list,
            "avg_items_per_language": round(avg_items_per_lang, 1),
            "avg_collections_per_language": round(avg_collections_per_lang, 1),
            "pct_languages_with_dates": round(pct_with_dates, 1),
            "num_with_iso_code": int(num_with_iso),
            "num_with_description": int(num_with_description),
            "num_with_indigenous_name": int(num_with_indigenous_name),
        })

    df = pd.DataFrame(results)

    # Compute a composite score for ranking
    # Normalize key metrics to 0-1 scale, then combine
    df = compute_composite_score(df)

    # Sort by composite score descending
    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    df.index = df.index + 1  # 1-based ranking
    df.index.name = "rank"

    return df


def compute_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a weighted composite score for family ranking.

    Weights reflect StoryMapJS storytelling priorities:
    - Temporal spread (year_span): 25% - key for chronological narrative
    - Geographic spread (num_countries): 20% - makes interesting map
    - Holdings richness (total_items): 20% - content for slides
    - Right size (penalize too small or too large): 20% - practical constraint
    - Data quality (pct_with_dates, descriptions): 15% - usable content
    """
    def normalize(series: pd.Series) -> pd.Series:
        """Min-max normalize to 0-1 range."""
        s_min, s_max = series.min(), series.max()
        if s_max == s_min:
            return pd.Series(0.5, index=series.index)
        return (series - s_min) / (s_max - s_min)

    def size_score(n: int) -> float:
        """
        Score family size: ideal is 10-30 languages.
        Peak score at 15-20, decreasing for very small or very large families.
        """
        if 10 <= n <= 30:
            # Peak around 15-20
            if 15 <= n <= 20:
                return 1.0
            elif n < 15:
                return 0.7 + 0.3 * (n - 10) / 5
            else:
                return 0.7 + 0.3 * (30 - n) / 10
        elif n < 10:
            return max(0, n / 10)
        else:  # n > 30
            return max(0.1, 1.0 - (n - 30) / 100)

    # Temporal spread score
    temporal_score = normalize(df["year_span"].fillna(0).astype(float))

    # Geographic spread score
    geo_score = normalize(df["num_countries"].astype(float))

    # Holdings richness score (log scale to reduce impact of outliers)
    import numpy as np
    holdings_score = normalize(np.log1p(df["total_items"].astype(float)))

    # Size score
    size_scores = df["num_languages"].apply(size_score)

    # Data quality score (weighted: dates matter more than descriptions)
    quality_score = normalize(
        df["pct_languages_with_dates"].fillna(0).astype(float) * 0.6 +
        (df["num_with_description"] / df["num_languages"].clip(lower=1) * 100) * 0.4
    )

    # Weighted composite
    df["score_temporal"] = (temporal_score * 100).round(1)
    df["score_geographic"] = (geo_score * 100).round(1)
    df["score_holdings"] = (holdings_score * 100).round(1)
    df["score_size"] = (size_scores * 100).round(1)
    df["score_quality"] = (quality_score * 100).round(1)

    df["composite_score"] = (
        temporal_score * 0.25 +
        geo_score * 0.20 +
        holdings_score * 0.20 +
        size_scores * 0.20 +
        quality_score * 0.15
    ).round(4) * 100

    df["composite_score"] = df["composite_score"].round(1)

    return df


def print_summary_table(df: pd.DataFrame) -> None:
    """Print a formatted summary table of all families."""
    print("\n" + "=" * 100)
    print("LANGUAGE FAMILY RANKINGS FOR STORYMAPJS")
    print("=" * 100)

    # Column headers
    header = (
        f"{'Rank':<5} {'Family':<35} {'Langs':>5} {'Items':>7} "
        f"{'Colls':>5} {'Years':>11} {'Span':>5} {'Countries':>4} "
        f"{'Score':>6}"
    )
    print(header)
    print("-" * 100)

    for rank, (_, row) in enumerate(df.iterrows(), 1):
        earliest = str(int(row["earliest_year"])) if row["earliest_year"] != "" else "----"
        latest = str(int(row["latest_year"])) if row["latest_year"] != "" else "----"
        year_range = f"{earliest}-{latest}"

        line = (
            f"{rank:<5} {row['family_name']:<35} {row['num_languages']:>5} "
            f"{row['total_items']:>7} {row['total_collections']:>5} "
            f"{year_range:>11} {row['year_span']:>5} {row['num_countries']:>4} "
            f"{row['composite_score']:>6.1f}"
        )
        print(line)

    print("-" * 100)
    print(f"Total families: {len(df)}")


def print_top_10_report(df: pd.DataFrame) -> None:
    """Print detailed analysis of top 10 candidates with commentary."""
    print("\n" + "=" * 100)
    print("TOP 10 CANDIDATES - DETAILED ANALYSIS")
    print("=" * 100)

    top_10 = df.head(10)

    for rank, (_, row) in enumerate(top_10.iterrows(), 1):
        print(f"\n{'─' * 80}")
        print(f"#{rank}: {row['family_name']}")
        print(f"{'─' * 80}")

        print(f"  Languages:    {row['num_languages']} total, "
              f"{row['num_languages_with_items']} with items, "
              f"{row['num_languages_with_collections']} with collections")
        print(f"  Items:        {row['total_items']:,} total items, "
              f"{row['total_collections']} collections")
        print(f"  Avg per lang: {row['avg_items_per_language']:.1f} items, "
              f"{row['avg_collections_per_language']:.1f} collections")

        if row["earliest_year"] != "":
            print(f"  Date range:   {int(row['earliest_year'])} - {int(row['latest_year'])} "
                  f"({row['year_span']} year span)")
        else:
            print(f"  Date range:   No temporal data")

        print(f"  Date coverage: {row['pct_languages_with_dates']:.0f}% of languages with items have dates")
        print(f"  Countries:    {row['num_countries']} ({row['countries_list']})")

        # Sub-scores
        print(f"  Scores:       temporal={row['score_temporal']:.0f} "
              f"geo={row['score_geographic']:.0f} "
              f"holdings={row['score_holdings']:.0f} "
              f"size={row['score_size']:.0f} "
              f"quality={row['score_quality']:.0f} "
              f"| COMPOSITE={row['composite_score']:.1f}")

        # Commentary
        commentary = generate_commentary(row)
        print(f"  Assessment:   {commentary}")


def generate_commentary(row: pd.Series) -> str:
    """Generate editorial commentary for a family based on its metrics."""
    strengths = []
    weaknesses = []
    flags = []

    # Temporal assessment
    if row["year_span"] >= 40:
        strengths.append("excellent temporal narrative arc")
    elif row["year_span"] >= 20:
        strengths.append("good temporal spread")
    elif row["year_span"] > 0:
        weaknesses.append("limited temporal range")
    else:
        weaknesses.append("NO temporal data")

    # Geographic assessment
    if row["num_countries"] >= 5:
        strengths.append(f"spans {row['num_countries']} countries (great map coverage)")
    elif row["num_countries"] >= 3:
        strengths.append(f"spans {row['num_countries']} countries")
    elif row["num_countries"] == 1:
        weaknesses.append("single country only")
    elif row["num_countries"] == 0:
        weaknesses.append("no geographic data")

    # Size assessment
    n = row["num_languages"]
    if 10 <= n <= 30:
        strengths.append(f"ideal size ({n} languages)")
    elif n < 5:
        flags.append(f"too small ({n} languages, hard to fill a map)")
    elif n > 50:
        flags.append(f"very large ({n} languages, needs careful curation)")

    # Holdings assessment
    if row["total_items"] >= 1000:
        strengths.append(f"rich holdings ({row['total_items']:,} items)")
    elif row["total_items"] >= 100:
        strengths.append("decent holdings")
    elif row["total_items"] < 10:
        weaknesses.append("very few items")

    # Date quality
    if row["pct_languages_with_dates"] >= 80:
        strengths.append("strong date coverage")
    elif row["pct_languages_with_dates"] < 30 and row["num_languages_with_items"] > 0:
        weaknesses.append("poor date coverage")

    parts = []
    if strengths:
        parts.append("Strengths: " + "; ".join(strengths))
    if weaknesses:
        parts.append("Weaknesses: " + "; ".join(weaknesses))
    if flags:
        parts.append("Flags: " + "; ".join(flags))

    return " | ".join(parts) if parts else "No notable characteristics"


def print_recommendations(df: pd.DataFrame) -> None:
    """Print final recommendations for family selection."""
    print("\n" + "=" * 100)
    print("SELECTION RECOMMENDATIONS")
    print("=" * 100)

    # Identify ideal candidates (10-30 languages, good temporal + geo spread)
    ideal = df[
        (df["num_languages"] >= 10) &
        (df["num_languages"] <= 30) &
        (df["year_span"] > 0) &
        (df["num_countries"] >= 3)
    ]

    print(f"\nFamilies meeting ALL ideal criteria (10-30 langs, dates, 3+ countries):")
    if len(ideal) > 0:
        for _, row in ideal.head(5).iterrows():
            print(f"  - {row['family_name']}: {row['num_languages']} langs, "
                  f"{row['year_span']}yr span, {row['num_countries']} countries, "
                  f"{row['total_items']:,} items (score: {row['composite_score']:.1f})")
    else:
        print("  None meet all criteria. Consider relaxing constraints.")

    # Families to avoid
    print(f"\nFamilies to AVOID (too few items, no dates, or single country):")
    avoid = df[
        (df["total_items"] < 10) |
        ((df["year_span"] == 0) & (df["num_countries"] <= 1))
    ]
    for _, row in avoid.iterrows():
        reasons = []
        if row["total_items"] < 10:
            reasons.append(f"only {row['total_items']} items")
        if row["year_span"] == 0:
            reasons.append("no temporal data")
        if row["num_countries"] <= 1:
            reasons.append(f"only {row['num_countries']} country")
        print(f"  - {row['family_name']}: {', '.join(reasons)}")

    # Geographic diversity check
    print(f"\nGeographic diversity tip:")
    print(f"  Choose families from different regions for variety. For example:")
    print(f"  - One Mesoamerican family (Mexico/Central America)")
    print(f"  - One South American family (Amazonia/Andes)")
    print(f"  - One family spanning both regions or including Caribbean")


def main():
    """Main execution: load data, analyze families, output rankings."""
    print("=" * 100)
    print("AILLA LANGUAGE FAMILY ANALYSIS FOR STORYMAPJS")
    print("=" * 100)

    # Load data
    languages, items, collections = load_data()

    # Verify temporal data is populated
    has_items = pd.to_numeric(languages["total_items"], errors="coerce").fillna(0)
    langs_with_items = (has_items > 0).sum()
    if langs_with_items == 0:
        print("\nWARNING: No languages have item data. The items extraction may not have run.")
        print("Run the full scraper first: uv run scripts/ailla_scraper.py")
        sys.exit(1)

    print(f"\n  Languages with items: {langs_with_items}")
    has_dates = languages["earliest_item_year"].notna() & (languages["earliest_item_year"] != "")
    print(f"  Languages with date ranges: {has_dates.sum()}")

    # Analyze families
    family_df = analyze_families(languages, items)

    # Save to CSV
    family_df.to_csv(OUTPUT_FILE, index=True, encoding="utf-8")
    print(f"\nFull rankings saved to: {OUTPUT_FILE}")

    # Print outputs
    print_summary_table(family_df)
    print_top_10_report(family_df)
    print_recommendations(family_df)

    print("\n" + "=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    main()
