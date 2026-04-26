# AdventureWorks BI Pipeline

Potok ETL/ELT zbudowany w oparciu o **DLT** i **dbt**, realizujący pełny przepływ danych od bazy źródłowej AdventureWorks2014 do schematu gwiazdy na SQL Server.

## Architektura

```
AdventureWorks2014 (MSSQL)
        │
        │  DLT  (data_extract.py)
        ▼
  aw-db  [schemat: extract]          ← surowe tabele + kursy NBP + oceny CSV
        │
        │  dbt  (adventure_works_dbt/)
        ▼
  aw-olap [schemat: staging]         ← modele pośrednie (stg_*)
  aw-olap [schemat: main]            ← schemat gwiazdy (dim_* + fact_sales)
```

### Schemat gwiazdy (aw-olap.main)

| Tabela              | Opis                                                                       |
| ------------------- | -------------------------------------------------------------------------- |
| `dim_product`     | Wymiar produktu z PROFIT, MARGIN, ACTIVE, SOLDFOR, DISCRETEPRICE i ocenami |
| `dim_salesperson` | Wymiar sprzedawcy z pełnym imieniem i denormalizowanymi danymi terytorium |
| `dim_territory`   | Wymiar terytorium sprzedaży z pełną nazwą kraju                        |
| `dim_date`        | Wymiar daty z atrybutami dzień/tydzień/miesiąc/kwartał/półrocze/rok  |
| `fact_sales`      | Tabela faktów sprzedaży (ziarno = linia zamówienia) z kwotą w PLN      |

---

## Wymagania wstępne

### System

- **Python 3.11** lub nowszy
- **Microsoft ODBC Driver 18 for SQL Server**
- **SQL Server** (wersja 2019 lub nowsza; można użyć kontenera Docker)
- Dostęp do internetu (API NBP — kursy USD/PLN)

#### Instalacja ODBC Driver 18 (Linux)

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
    | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

#### Instalacja ODBC Driver 18 (Windows)

Pobierz i zainstaluj ze strony Microsoft:
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### SQL Server — wymagane bazy danych

Na serwerze muszą istnieć trzy bazy danych:

| Baza                   | Rola                                            |
| ---------------------- | ----------------------------------------------- |
| `AdventureWorks2014` | Źródło danych (baza OLTP)                    |
| `aw-db`              | Obszar staging — DLT zapisuje tu surowe tabele |
| `aw-olap`            | Cel — dbt buduje tu schemat gwiazdy            |

Bazy `aw-db` i `aw-olap` można utworzyć jednym skryptem:

```sql
CREATE DATABASE [aw-db];
CREATE DATABASE [aw-olap];
```

Użytkownik bazy danych musi mieć uprawnienia `db_owner` (lub równoważne `CREATE TABLE`, `ALTER`, `INSERT`, `SELECT`, `DROP`) na obu bazach `aw-db` i `aw-olap`, oraz `SELECT` na `AdventureWorks2014`.

---

## Instalacja krok po kroku

### 1. Klonowanie repozytorium

```bash
git clone <adres-repozytorium>
cd AdventureWorks-ELT
```

### 2. Środowisko Python

```bash
python3 -m venv .venv_
source .venv_/bin/activate        # Linux / macOS
# .venv_\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 3. Konfiguracja zmiennych środowiskowych

Skopiuj plik przykładowy i uzupełnij go danymi swojego serwera:

```bash
cp .env.example .env
```

Edytuj `.env` — zmień `your_password` na swoje hasło oraz w razie potrzeby dostosuj host, port i nazwy baz:

```env
# Dane logowania są wspólne dla DLT i dbt — wystarczy wypełnić raz.

SOURCES__SQL_DATABASE__CREDENTIALS__HOST=127.0.0.1
SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD=twoje_haslo
# ... (pozostałe wartości w pliku .env.example)

DBT_SERVER=127.0.0.1
DBT_PASSWORD=twoje_haslo
# ...
```

UWAGA! W przypadku błędu dlt najlepiej jest skopiować przykłądowy plik i uzupełnić go danymi. Rozwiązuje to większość problemów z odczytem danych przez dlt z .env.

```
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Plik `.env` jest wymieniony w `.gitignore` — **nigdy nie zostanie zacommitowany**.

> **Jak to działa:**
>
> - DLT odczytuje zmienne `SOURCES__*` i `DESTINATION__*` automatycznie z otoczenia.
> - dbt odczytuje zmienne `DBT_*` przez wywołania `env_var()` w `profiles.yml`.
> - Skrypty Python ładują `.env` przez `python-dotenv` na starcie.

### 4. Konfiguracja dbt — profil połączenia

Profil dbt musi znajdować się w `~/.dbt/profiles.yml` (katalog domowy użytkownika, **poza** repozytorium).

Skopiuj plik przykładowy:

```bash
cp profiles.yml.example ~/.dbt/profiles.yml
```

Profil korzysta ze zmiennych `DBT_*` zdefiniowanych w `.env` — nie musisz ręcznie edytować `profiles.yml`.

---

## Uruchomienie potoku

Wszystkie poniższe komendy wykonuj z głównego katalogu repozytorium (`AdventureWorks-ELT/`) przy aktywnym środowisku wirtualnym.

### Krok 1 — Ekstrakcja danych źródłowych i kursów walut

```bash
python data_extract.py
```

Skrypt wykonuje trzy operacje:

1. Kopiuje tabele z `AdventureWorks2014` do `aw-db.extract` (DLT)
2. Ładuje plik `SBI2526-LAB-Rating-FixedDate.csv` z ocenami produktów do `aw-db.extract`
3. Pobiera historyczne kursy USD/PLN z API NBP (lata 2011–2014) i zapisuje je do `aw-db.extract`

Oczekiwany wynik: ~176 000 wierszy w ~11 tabelach w schemacie `aw-db.extract`.

### Krok 2 — Budowa modeli dbt

```bash
cd adventure_works_dbt
dbt run
```

dbt zbuduje po kolei 11 modeli:

```
staging.stg_product
staging.stg_salesperson
staging.stg_territory
staging.stg_sales_order
staging.stg_date
staging.stg_exchange_rate
main.dim_product
main.dim_salesperson
main.dim_territory
main.dim_date
main.fact_sales
```

Oczekiwany wynik: `PASS=11 WARN=0 ERROR=0`.

### Krok 3 — Weryfikacja testów jakości danych

```bash
dbt test
```

Wykonuje 94 testy obejmujące:

- unikalność i brak NULLi na wszystkich kluczach
- poprawność wartości (`accepted_values`) dla `active`, `discrete_price`, `half_year`, `rate_change_direction`
- integralność referencyjną (FK) między `fact_sales` a wszystkimi wymiarami

Oczekiwany wynik: `PASS=94 WARN=0 ERROR=0`.

---

## Struktura repozytorium

```
AdventureWorks-ELT/
├── .env                             # sekrety połączeń (NIE commitować — git-ignored)
├── .env.example                     # szablon do skopiowania
│
├── .dlt/
│   ├── config.toml                  # konfiguracja runtime DLT
│   ├── secrets.toml                 # fallback TOML (git-ignored; .env ma pierwszeństwo)
│   └── secrets.toml.example         # dokumentacja formatu TOML
│
├── adventure_works_dbt/
│   ├── dbt_project.yml              # konfiguracja projektu dbt
│   ├── macros/
│   │   ├── cross_db.sql             # makra SQL (bool_cast, date_to_int, itp.)
│   │   └── generate_schema_name.sql # override nazwy schematu dbt
│   └── models/
│       ├── staging/
│       │   ├── src_adventureworks.yml   # deklaracje źródeł dbt
│       │   ├── stg_schema.yml           # testy modeli staging
│       │   ├── stg_product.sql
│       │   ├── stg_salesperson.sql
│       │   ├── stg_territory.sql
│       │   ├── stg_sales_order.sql
│       │   ├── stg_date.sql
│       │   └── stg_exchange_rate.sql
│       └── marts/
│           ├── marts_schema.yml         # testy modeli marts
│           ├── dim_product.sql
│           ├── dim_salesperson.sql
│           ├── dim_territory.sql
│           ├── dim_date.sql
│           └── fact_sales.sql
│
├── data_extract.py                  # potok DLT: AW2014 + CSV + kursy NBP → aw-db
├── exchange_rates.py                # moduł pobierania kursów USD/PLN z NBP
├── SBI2526-LAB-Rating-FixedDate.csv # oceny produktów (źródło zewnętrzne)
├── profiles.yml.example             # szablon profilu dbt (używa env_var())
└── requirements.txt                 # zależności Python
```

---

## Najczęstsze problemy

### `KeyError: 'DBT_PASSWORD'` lub brak połączenia po sklonowaniu

Plik `.env` nie istnieje lub jest pusty. Wykonaj:

```bash
cp .env.example .env
# następnie wypełnij .env swoim hasłem i adresem serwera
```

### `[08001] Named Pipes Provider: Could not open a connection`

Serwer SQL jest nieosiągalny. Sprawdź adres, port (domyślnie 1433) i czy usługa SQL Server jest uruchomiona.

### `Login failed for user 'sa'`

Nieprawidłowe hasło lub konto `sa` jest wyłączone. Upewnij się, że SQL Server jest w trybie uwierzytelniania mieszanego (SQL + Windows).

### `Catalog Error: Schema "extract" does not exist`

Skrypt `data_extract.py` nie został uruchomiony przed `dbt run`, albo zakończył się błędem. Uruchom go ponownie i sprawdź logi.

### `dbt: command not found`

dbt jest zainstalowany wewnątrz środowiska wirtualnego. Aktywuj je przed wywołaniem dbt:

```bash
source .venv_/bin/activate
```

### Testy `not_null` na kursach walut

Kursy NBP obejmują lata 2011–2014 zgodnie z zakresem dat w AdventureWorks2014. Jeśli baza źródłowa zawiera inny zakres dat zamówień, zmień zmienne `EXCHANGE_START_DATE` i `EXCHANGE_END_DATE` w pliku `.env`.
