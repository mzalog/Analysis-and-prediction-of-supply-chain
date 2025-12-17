# Big Data Supply Chain – Digital Twin & Analytics Platform

Projekt symulatora i platformy analitycznej dla łańcucha dostaw (Supply Chain Digital Twin). System łączy generowanie syntetycznych danych behawioralnych (symulacja agentowa) z klasycznym potokiem przetwarzania Big Data (ETL, EDA, ML-ready).

---

## 🚀 Szybki Start (Symulacja)

Najciekawszą częścią projektu jest **interaktywna symulacja** logistyki.

### Uruchomienie (Windows)
```powershell
.\run_simulation.ps1
```

To polecenie uruchomi:
1.  Generowanie losowego grafu logistycznego (magazyny, klienci, porty).
2.  Symulację ruchu ciężarówek i realizacji zamówień w czasie rzeczywistym.
3.  **Wizualizację na żywo** (mapa, ciężarówki, statystyki).

---

## 🌟 Główne Funkcjonalności

### 1. Symulator Łańcucha Dostaw
Silnik symulacji zdarzeń dyskretnych (Discrete Event Simulation) modelujący rzeczywiste procesy logistyczne.
*   **Agenty**: Ciężarówki z cyklem życia (IDLE, EN_ROUTE, RESTING).
*   **Graf**: Topologia sieci z różnymi typami węzłów (Warehouse, Hub, Customer, Port, Inspection).
*   **Logika**:
    *   Generowanie zamówień.
    *   Przydzielanie zadań (Dispatcher).
    *   Model opóźnień (korki, pogoda, czas obsługi).
    *   Obowiązkowe przerwy dla kierowców.
*   **Wizualizacja**:
    *   Interaktywna mapa `matplotlib`.
    *   Rozróżnienie typów węzłów (kolory/kształty).
    *   Wskaźniki oczekujących zamówień.
    *   Dashboard (HUD) ze statystykami w czasie rzeczywistym.

### 2. Pipeline Danych
Kompletny proces ETL przygotowujący dane do analizy i uczenia maszynowego.
*   **Czyszczenie**: Normalizacja typów, usuwanie duplikatów.
*   **Walidacja (Pandera)**: Sprawdzanie jakości danych (zakresy, typy, braki) zgodnie ze schematem `SupplyChainSchema`.
*   **Feature Engineering**:
    *   Cechy czasowe (dzień tygodnia, godzina).
    *   Lagi i okna kroczące (rolling window) dla statusu zamówień.
*   **Preprocessing**: Imputacja braków i skalowanie (`StandardScaler`) gotowe pod ML.
*   **Split**: Podział chronologiczny na zbiory Train/Val/Test.

### 3. Analiza Eksploracyjna (EDA)
Automatyczne generowanie raportów i wykresów.
*   Macierze korelacji.
*   Analiza braków danych.
*   Rozkłady zmiennych.
*   Eksport podsumowań do CSV i PNG w katalogu `reports/`.

---

## 🛠️ Instalacja

Wymagany Python **3.9+**.

1.  Utwórz i aktywuj wirtualne środowisko (zalecane):
    ```bash
    python -m venv .venv
    .venv\Scripts\activate
    ```
2.  Zainstaluj zależności:
    ```bash
    pip install -r requirements.txt
    ```

---

## 📖 Szczegółowa Instrukcja Użycia

### A. Symulacja (Digital Twin)

Uruchomienie symulacji z poziomu CLI:

```bash
# Tryb Live (z wizualizacją)
python -m supply_chain.cli.main --simulate --live

# Tryb Headless (szybkie generowanie danych bez okna)
python -m supply_chain.cli.main --simulate
```

**Wyniki symulacji:**
*   `data/raw/simulation_events.csv`: Pełny dziennik zdarzeń (Event Log).
*   `data/raw/simulated_kaggle_compatible.csv`: Dane przekonwertowane do formatu kompatybilnego z datasetem Kaggle (do dalszej analizy w pipeline).
*   `reports/figures/simulation_*.png`: Wykresy podsumowujące (graf, oś czasu).

### B. Pipeline Przetwarzania Danych (ETL)

Uruchomienie pełnego procesu przetwarzania na danych (oryginalnych lub z symulacji):

```bash
python -m supply_chain.cli.main --csv-path data/raw/dynamic_supply_chain_logistics_dataset.csv
```

**Opcje CLI:**
*   `--no-validation`: Pomiń walidację jakości (Pandera).
*   `--no-time-features`: Pomiń generowanie cech czasowych.
*   `--no-preprocessing`: Pomiń imputację i skalowanie.
*   `--no-time-split`: Pomiń podział na zbiory treningowe.
*   `--sample-frac 0.1`: Przetwarzaj tylko 10% danych (do szybkich testów).

---

## 📂 Struktura Projektu

```text
.
├── data/
│   ├── raw/            # Surowe dane (CSV) i wyniki symulacji
│   ├── interim/        # Dane oczyszczone (Parquet)
│   └── processed/      # Dane gotowe do ML (Train/Val/Test)
├── reports/
│   ├── figures/        # Wykresy (EDA, Symulacja)
│   └── *.csv           # Raporty jakości danych
├── src/
│   └── supply_chain/
│       ├── cli/        # Punkt wejścia (main.py)
│       ├── data/       # Logika ETL (Loader, Cleaner, Validator, Preprocessor)
│       ├── eda/        # Analiza danych
│       ├── simulation/ # SILNIK SYMULACJI (Sprint 4)
│       │   ├── engine.py        # Logika zdarzeń
│       │   ├── graph.py         # Generowanie sieci
│       │   ├── visualization.py # Wizualizacja (Matplotlib)
│       │   └── schema.py        # Modele domenowe (Truck, Order, Node)
│       └── schemas.py  # Schematy walidacji (Pandera)
├── tests/              # Testy jednostkowe (pytest)
├── run_simulation.ps1  # Skrypt pomocniczy
└── requirements.txt
```

---

## ✅ Testy

Projekt posiada zestaw testów jednostkowych weryfikujących kluczowe komponenty.

Uruchomienie wszystkich testów:
```bash
pytest tests/
```

Kluczowe testy:
*   `tests/verify_enhanced_viz.py`: Weryfikacja wizualizacji symulacji.
*   `tests/test_schemas.py`: Sprawdzenie poprawności walidacji danych.
*   `tests/test_preprocessing.py`: Testy pipeline'u ML.
