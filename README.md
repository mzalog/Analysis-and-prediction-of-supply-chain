# Big Data Supply Chain – Digital Twin & Analytics Platform

Projekt symulatora i platformy analitycznej dla łańcucha dostaw (Supply Chain Digital Twin). System łączy generowanie syntetycznych danych behawioralnych (symulacja agentowa / DES) z potokiem przetwarzania danych w duchu Big Data (ETL, EDA, dane gotowe pod ML).

## Zakres projektu

W repozytorium znajdują się trzy kluczowe obszary:

- Symulator łańcucha dostaw (Discrete Event Simulation) generujący event log.
- Pipeline danych (czyszczenie, walidacja Pandera, cechy, preprocessing, split czasowy).
- Narzędzia analityczne (EDA, wykresy/raporty) oraz komponenty ML.

## Szybki start

Poniższe komendy zakładają uruchomienie z katalogu głównego repozytorium.

### 1) Instalacja (zalecane)

Wymagany Python 3.9+.

```bash
python -m venv .venv
```

Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Instalacja projektu wraz z zależnościami (tryb developerski, zalecane dla `src/` layout):

```bash
pip install -e .
```

Opcjonalnie (narzędzia dev i testy):

```bash
pip install -e ".[dev]"
```

Alternatywnie (zgodnie z `requirements.txt`):

```bash
pip install -r requirements.txt
```

### 2) Symulacja (Digital Twin)

Tryb z wizualizacją:

```bash
python -m supply_chain.cli.main --simulate --live
```

Tryb headless (szybkie generowanie danych bez okna):

```bash
python -m supply_chain.cli.main --simulate
```

### 3) ETL / przygotowanie danych

Przetworzenie danych wejściowych (oryginalnych lub z symulacji):

```bash
python -m supply_chain.cli.main --csv-path data/raw/dynamic_supply_chain_logistics_dataset.csv
```

### 4) Testy

```bash
pytest
```

## Funkcjonalności

### Symulator łańcucha dostaw (DES)

Silnik symulacji zdarzeń dyskretnych modelujący procesy logistyczne i generujący strumień zdarzeń.

- Agenty: ciężarówki z cyklem życia (IDLE, EN_ROUTE, RESTING).
- Graf: topologia sieci z typami węzłów (Warehouse, Hub, Customer, Port, Inspection).
- Logika: zamówienia, dyspozytor (dispatcher), opóźnienia (korki/pogoda/czas obsługi), przerwy kierowców.
- Wizualizacja: mapa i statystyki w czasie rzeczywistym (matplotlib).

### Pipeline danych (ETL)

Kompletny proces przygotowania danych do analizy i uczenia maszynowego.

- Czyszczenie: normalizacja typów, usuwanie duplikatów.
- Walidacja Pandera: kontrola typów, zakresów i braków zgodnie ze schematem.
- Feature engineering: cechy czasowe, lagi i okna kroczące.
- Preprocessing: imputacja braków i skalowanie (`StandardScaler`).
- Split: podział chronologiczny Train/Val/Test.

### EDA

Automatyczne generowanie raportów i wykresów:

- Macierze korelacji.
- Analiza braków danych.
- Rozkłady zmiennych.
- Eksport podsumowań do CSV i PNG w katalogu `reports/`.

## Artefakty i wyniki

Wyniki symulacji i przetwarzania trafiają do standardowych katalogów:

- `data/raw/simulation_events.csv`: pełny dziennik zdarzeń (event log).
- `data/raw/simulated_kaggle_compatible.csv`: dane w formacie kompatybilnym z wejściem pipeline.
- `data/interim/`: dane po czyszczeniu (np. Parquet).
- `data/processed/`: dane gotowe pod ML (Train/Val/Test).
- `reports/figures/`: wykresy i wizualizacje (EDA oraz symulacja).

## CLI – najważniejsze opcje

Przykład przetwarzania danych:

```bash
python -m supply_chain.cli.main --csv-path data/raw/dynamic_supply_chain_logistics_dataset.csv
```

Wybrane flagi:

| Flaga | Znaczenie |
| --- | --- |
| `--simulate` | Uruchamia symulację (Digital Twin). |
| `--live` | Włącza wizualizację na żywo podczas symulacji. |
| `--csv-path <plik>` | Wskazuje plik CSV jako wejście dla ETL/EDA. |
| `--no-validation` | Pomija walidację jakości (Pandera). |
| `--no-time-features` | Pomija generowanie cech czasowych. |
| `--no-preprocessing` | Pomija imputację i skalowanie. |
| `--no-time-split` | Pomija podział czasowy na zbiory. |
| `--sample-frac <ułamek>` | Przetwarza podpróbkę danych (np. `0.1`). |

## Struktura projektu

```text
.
├── data/
│   ├── raw/            # Surowe dane (CSV) i wyniki symulacji
│   ├── interim/        # Dane oczyszczone (np. Parquet)
│   └── processed/      # Dane gotowe do ML (Train/Val/Test)
├── reports/
│   ├── figures/        # Wykresy (EDA, Symulacja)
│   └── *.csv           # Raporty jakości danych
├── src/
│   └── supply_chain/
│       ├── cli/        # Punkt wejścia (main.py)
│       ├── data/       # Logika ETL
│       ├── eda/        # Analiza danych
│       ├── simulation/ # Silnik symulacji
│       │   ├── engine.py        # Logika zdarzeń
│       │   ├── graph.py         # Generowanie sieci
│       │   ├── visualization.py # Wizualizacja (Matplotlib)
│       │   └── schema.py        # Modele domenowe (Truck, Order, Node)
│       └── schemas.py  # Schematy walidacji (Pandera)
└── tests/              # Testy jednostkowe (pytest)
```

## Troubleshooting

- Jeśli `python -m supply_chain.cli.main ...` nie znajduje modułu `supply_chain`, upewnij się, że wykonałeś `pip install -e .`.
- Jeśli w systemie nie działa aktywacja środowiska PowerShell, uruchom terminal jako Administrator lub zmień politykę wykonywania skryptów zgodnie z zasadami organizacji.

