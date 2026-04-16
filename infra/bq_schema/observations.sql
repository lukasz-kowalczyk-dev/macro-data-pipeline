-- =============================================================================
-- BigQuery Schema — tabela observations
-- Projekt: macro-data-pipeline
--
-- Uruchomienie:
--   bq query --project_id=TWOJ_PROJEKT --use_legacy_sql=false < observations.sql
--
-- Lub przez Python (loader.py wywołuje ensure_table_exists() automatycznie).
-- =============================================================================

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.observations`
(
    -- -------------------------------------------------------------------------
    -- Identyfikacja serii czasowej
    -- -------------------------------------------------------------------------

    source          STRING  NOT NULL,
    -- Źródło danych: 'OECD' lub 'IMF'

    dataset_code    STRING  NOT NULL,
    -- Kod datasetu w API źródłowym.
    -- Przykłady: 'QNA' (OECD Quarterly National Accounts), 'WEO' (IMF World Economic Outlook)

    series_id       STRING  NOT NULL,
    -- Unikalny klucz serii. Format: '{source}.{dataset_code}.{indicator_code}.{country_code}'
    -- Przykład: 'OECD.QNA.GDP.POL', 'IMF.WEO.NGDP_RPCH.USA'
    -- Używany w MERGE jako klucz upsert (razem z obs_date)

    indicator_code  STRING  NOT NULL,
    -- Kod wskaźnika makroekonomicznego.
    -- Przykłady OECD: 'GDP', 'GDPV', 'CPALTT01'
    -- Przykłady IMF:  'NGDP_RPCH' (real GDP growth), 'PCPIPCH' (CPI inflation)

    country_code    STRING  NOT NULL,
    -- Kod kraju w standardzie ISO 3166 alpha-3.
    -- Przykłady: 'POL', 'USA', 'DEU', 'GBR', 'CHN'

    frequency       STRING,
    -- Częstotliwość obserwacji:
    -- 'A' = Annual (roczna)
    -- 'Q' = Quarterly (kwartalna)
    -- 'M' = Monthly (miesięczna)

    -- -------------------------------------------------------------------------
    -- Wartość obserwacji
    -- -------------------------------------------------------------------------

    obs_date        DATE    NOT NULL,
    -- Data obserwacji — zawsze pierwszy dzień okresu.
    -- Roczne:     2023-01-01 (rok 2023)
    -- Kwartalne:  2023-01-01 (Q1), 2023-04-01 (Q2), 2023-07-01 (Q3), 2023-10-01 (Q4)
    -- Miesięczne: 2023-01-01, 2023-02-01, ..., 2023-12-01

    obs_value       FLOAT64,
    -- Wartość numeryczna obserwacji.
    -- NULL = dane niedostępne (API zwraca brak wartości lub "n/a")

    unit            STRING,
    -- Jednostka miary.
    -- Przykłady: '%', 'USD', 'USD_CAP', '% of GDP', 'INDEX'

    -- -------------------------------------------------------------------------
    -- Metadane
    -- -------------------------------------------------------------------------

    ingested_at     TIMESTAMP NOT NULL
    -- Timestamp (UTC) kiedy rekord został załadowany do BigQuery.
    -- Przydatne do audytu i monitorowania świeżości danych.

)

-- =============================================================================
-- Partycjonowanie
-- =============================================================================
-- Partycjonujemy po obs_date (miesięcznie).
-- Zaleta: BigQuery skanuje TYLKO partycje pasujące do filtra WHERE obs_date BETWEEN ...
-- Oszczędza czas i pieniądze przy zapytaniach zakresowych.
PARTITION BY DATE_TRUNC(obs_date, MONTH)

-- =============================================================================
-- Clustering
-- =============================================================================
-- Dane są fizycznie grupowane na dysku wg tych kolumn (w tej kolejności).
-- Przyspiesza filtrowanie i GROUP BY bez pełnego skanu.
-- Typowe zapytania analityczne: WHERE source='OECD' AND country_code='POL' AND indicator_code='GDP'
CLUSTER BY source, country_code, indicator_code

-- =============================================================================
-- Opcje tabeli
-- =============================================================================
OPTIONS (
    description = "Dane makroekonomiczne OECD i IMF — płaska tabela szeregów czasowych",
    -- Czas życia partycji: NULL = bez automatycznego usuwania
    partition_expiration_days = NULL
);


-- =============================================================================
-- Przykładowe zapytania analityczne
-- =============================================================================

-- 1. Inflacja CPI w Polsce, Niemczech i USA za ostatnie 5 lat (roczna):
--
-- SELECT country_code, EXTRACT(YEAR FROM obs_date) AS year, obs_value
-- FROM `{project}.{dataset}.observations`
-- WHERE indicator_code = 'PCPIPCH'
--   AND country_code IN ('POL', 'DEU', 'USA')
--   AND obs_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
-- ORDER BY country_code, year;


-- 2. Porównanie wzrostu PKB między krajami (IMF WEO, dane roczne):
--
-- SELECT
--   country_code,
--   EXTRACT(YEAR FROM obs_date) AS year,
--   obs_value AS gdp_growth_pct
-- FROM `{project}.{dataset}.observations`
-- WHERE source = 'IMF'
--   AND dataset_code = 'WEO'
--   AND indicator_code = 'NGDP_RPCH'
--   AND obs_date BETWEEN '2010-01-01' AND '2023-12-31'
-- ORDER BY year, country_code;


-- 3. Sprawdzenie świeżości danych (kiedy ostatnio załadowano):
--
-- SELECT source, dataset_code, MAX(ingested_at) AS last_ingested
-- FROM `{project}.{dataset}.observations`
-- GROUP BY source, dataset_code
-- ORDER BY last_ingested DESC;
