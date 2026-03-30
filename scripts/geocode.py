"""Geocode AILLA languages with latitude/longitude coordinates.

Assigns geographic coordinates to languages in the Mayan and Quechua families
for StoryMapJS visualization. Uses curated coordinates based on known language-
speaking regions rather than automated geocoding, since indigenous language areas
often don't align with city or country centroids.

For languages spoken across multiple countries, coordinates are placed in the
primary speaking region. Small random offsets (jitter) are applied to prevent
pins from stacking on top of each other when multiple languages share a region.

Usage:
    uv run scripts/geocode.py

Output:
    Updates data/languages_dataset.csv with 'latitude' and 'longitude' columns.
"""

import pandas as pd
import random
from pathlib import Path

# Reproducible jitter for consistent results across runs
random.seed(42)

# --- Curated coordinates for Mayan language-speaking regions ---
# Sources: Ethnologue, Glottolog, AILLA collection descriptions
# Coordinates target the primary speaking community, not country centroids.

MAYAN_COORDS: dict[str, tuple[float, float]] = {
    # Tzeltal - Chiapas highlands, Mexico (Ocosingo/Altamirano area)
    "tzh": (16.78, -92.10),
    # Ch'ol - northern Chiapas, Mexico (Tila/Tumbalá)
    "ctu": (17.30, -92.40),
    # Ch'olti' - eastern Guatemala / Belize border (historical, now extinct)
    "mis": (15.20, -89.20),
    # Chorti - Chiquimula department, eastern Guatemala
    "caa": (14.80, -89.10),
    # Chuj - Huehuetenango, Guatemala / Chiapas border
    "cac": (15.90, -91.60),
    # Tzotzil - Chiapas highlands, Mexico (San Cristóbal area)
    "tzo": (16.73, -92.63),
    # Q'eqchi' - Alta Verapaz, Guatemala / southern Belize
    "kek": (15.47, -89.90),
    # Achi - Baja Verapaz, Guatemala (Rabinal/Cubulco)
    "acr": (15.08, -90.50),
    # K'ichee' - central highlands Guatemala (Quetzaltenango/Sololá)
    "quc": (14.83, -91.52),
    # Tojolab'al - eastern Chiapas, Mexico (Las Margaritas)
    "toj": (16.30, -91.98),
    # Q'anjob'al - Huehuetenango highlands, Guatemala
    "kjb": (15.70, -91.45),
    # Poqomchi' - Alta Verapaz / Baja Verapaz, Guatemala
    "poh": (15.30, -90.30),
    # Popti' (Jakalteko) - Huehuetenango, Guatemala / Chiapas
    "jac": (15.65, -91.70),
    # Maya, Yucatec - Yucatan Peninsula, Mexico / northern Belize
    "yua": (20.50, -89.00),
    # Ixil - Quiché department, Guatemala (Nebaj/Chajul/Cotzal)
    "ixl": (15.40, -91.15),
    # Awakateko - Huehuetenango, Guatemala (Aguacatán)
    "agu": (15.34, -91.31),
    # Chontal, Tabasco - Tabasco lowlands, Mexico
    "chf": (18.10, -93.00),
    # Huasteco (Teenek) - San Luis Potosí / Veracruz, Mexico
    "hus": (21.30, -98.80),
    # Lacandon - Chiapas lowlands, Mexico (Lacandón jungle)
    "lac": (16.90, -91.10),
    # Mocho / Tuzanteco - Motozintla area, Chiapas, Mexico
    # NOTE: Mocho and Tuzanteco share ISO code mhc; they are closely related
    # varieties. We place them near each other with slight offset.
    "mhc": (15.37, -92.25),
    # Tektiteko - Huehuetenango, Guatemala / Chiapas border (Tectitán)
    "ttc": (15.31, -92.05),
    # Mam - western highlands Guatemala (Huehuetenango / San Marcos)
    "mam": (15.20, -91.80),
    # Tz'utujil - Lake Atitlán, Guatemala (Santiago Atitlán)
    "tzj": (14.63, -91.23),
    # Chicomuceltec - Chiapas, Mexico (near Comitán, now extinct)
    "cob": (16.25, -92.10),
    # Poqomam - Guatemala (Jalapa / eastern Guatemala City area)
    "poc": (14.60, -89.98),
    # Mopán - Petén, Guatemala / Belize (San Luis/Toledo)
    "mop": (16.20, -89.30),
    # Kaqchikel - central Guatemala (Chimaltenango / Sololá)
    "cak": (14.65, -90.82),
    # Uspanteko - Quiché, Guatemala (Uspantán)
    "usp": (15.35, -90.87),
    # Sakapulteko - Quiché, Guatemala (Sacapulas)
    "quv": (15.29, -91.09),
    # Akateko - Huehuetenango, Guatemala (San Miguel Acatán)
    "knj": (15.70, -91.60),
    # Itza' - Petén, Guatemala (Flores / Lake Petén Itzá)
    "itz": (16.93, -89.89),
    # Sipakapense - San Marcos, Guatemala (Sipacapa)
    "qum": (15.22, -91.63),
}

# --- Curated coordinates for Quechua language-speaking regions ---
QUECHUA_COORDS: dict[str, tuple[float, float]] = {
    # Quechua, South Bolivian - Bolivia (Cochabamba/Sucre) and NW Argentina
    "quh": (-17.39, -66.16),
    # Inga - southern Colombia (Putumayo / Nariño)
    "inb": (1.15, -77.28),
    # Quechua, Pastaza - eastern Ecuador / northeastern Peru (Amazon)
    "qvz": (-2.30, -76.95),
    # Kichwa, Cañar Highland - southern Ecuador (Cañar province)
    "qxr": (-2.56, -79.00),
    # Quechua, Cuzco - Cusco region, Peru
    "quz": (-13.52, -71.97),
    # Quechua, Ancash Huayllas - Ancash region, Peru (Callejón de Huaylas)
    "qwh": (-9.50, -77.53),
    # Quechua, Chachapoyas - Amazonas region, Peru
    "quk": (-6.23, -77.87),
    # Kichwa, Loja Highland - southern Ecuador (Loja / Saraguro)
    "qvj": (-3.80, -79.60),
    # Napo Quichua - eastern Ecuador / Colombia / Peru (Napo River)
    "qvo": (-0.90, -77.80),
    # Quechua, Ayacucho - Ayacucho region, Peru
    "quy": (-13.16, -74.22),
    # Quechua, Huallaga - Huánuco region, Peru (Huallaga valley)
    "qub": (-9.40, -76.00),
    # Quechua, Huamalíes-Huánuco - Huánuco, Peru
    "qvh": (-9.93, -76.24),
    # Quechua, Puno - Puno region, southern Peru
    "qxp": (-15.84, -70.02),
    # Quechua, San Martín - San Martín region, Peru (Lamas)
    "qvs": (-6.42, -76.52),
    # Quechua, Yauyos - Lima region, Peru (Yauyos province)
    "qux": (-12.49, -75.91),
    # Quichua, Imbabura - northern Ecuador (Otavalo / Imbabura)
    "qvi": (0.30, -78.26),
    # Quichua, Tena Lowland - eastern Ecuador (Napo / Tena)
    "quw": (-0.99, -77.81),
}


def add_jitter(lat: float, lon: float, scale: float = 0.08) -> tuple[float, float]:
    """Add small random offset to prevent pin stacking.

    Args:
        lat: Base latitude.
        lon: Base longitude.
        scale: Maximum offset in degrees (~0.08 degrees = ~9 km).

    Returns:
        Tuple of (jittered_lat, jittered_lon).
    """
    jlat = lat + random.uniform(-scale, scale)
    jlon = lon + random.uniform(-scale, scale)
    return round(jlat, 4), round(jlon, 4)


def geocode_languages(csv_path: str) -> pd.DataFrame:
    """Add latitude and longitude columns to the languages dataset.

    Languages in the Mayan and Quechua families receive curated coordinates
    with small jitter offsets. All other languages receive NaN for now.

    Args:
        csv_path: Path to languages_dataset.csv.

    Returns:
        Updated DataFrame with latitude and longitude columns.
    """
    df = pd.read_csv(csv_path)

    # Initialize coordinate columns
    df["latitude"] = pd.NA
    df["longitude"] = pd.NA

    # Combined lookup
    all_coords = {**MAYAN_COORDS, **QUECHUA_COORDS}

    # Track ISO codes we've already placed (for shared codes like mhc)
    placed_codes: dict[str, int] = {}

    geocoded_count = 0
    skipped = []

    for idx, row in df.iterrows():
        iso = row["iso_639_3_code"]
        family = row["language_family"]

        if family not in ("Mayan", "Quechua"):
            continue

        if iso in all_coords:
            base_lat, base_lon = all_coords[iso]

            # If this ISO code has been placed before (e.g., Mocho/Tuzanteco
            # sharing mhc), increase jitter to separate them
            times_placed = placed_codes.get(iso, 0)
            if times_placed > 0:
                jitter_scale = 0.15
            else:
                jitter_scale = 0.08

            lat, lon = add_jitter(base_lat, base_lon, scale=jitter_scale)
            df.at[idx, "latitude"] = lat
            df.at[idx, "longitude"] = lon
            placed_codes[iso] = times_placed + 1
            geocoded_count += 1
        else:
            skipped.append(f"  {row['name_en']} ({iso}) - {family}")

    print(f"Geocoded {geocoded_count} languages")
    if skipped:
        print(f"Skipped {len(skipped)} languages (no coordinates defined):")
        for s in skipped:
            print(s)

    return df


def main() -> None:
    """Geocode Mayan and Quechua languages and update the dataset."""
    data_dir = Path(__file__).parent.parent / "data"
    csv_path = data_dir / "languages_dataset.csv"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found. Run ailla_scraper.py first.")
        return

    print("Geocoding AILLA languages...")
    print(f"Reading {csv_path}")

    df = geocode_languages(str(csv_path))

    # Save updated dataset
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"\nSaved updated dataset to {csv_path}")

    # Summary
    mayan = df[df["language_family"] == "Mayan"]
    quechua = df[df["language_family"] == "Quechua"]
    mayan_geocoded = mayan["latitude"].notna().sum()
    quechua_geocoded = quechua["latitude"].notna().sum()

    print(f"\nMayan: {mayan_geocoded}/{len(mayan)} languages geocoded")
    print(f"Quechua: {quechua_geocoded}/{len(quechua)} languages geocoded")

    # Show coordinate ranges for verification
    for name, subset in [("Mayan", mayan), ("Quechua", quechua)]:
        geo = subset[subset["latitude"].notna()]
        if not geo.empty:
            print(f"\n{name} coordinate bounds:")
            print(f"  Lat: {geo['latitude'].min():.2f} to {geo['latitude'].max():.2f}")
            print(f"  Lon: {geo['longitude'].min():.2f} to {geo['longitude'].max():.2f}")


if __name__ == "__main__":
    main()
