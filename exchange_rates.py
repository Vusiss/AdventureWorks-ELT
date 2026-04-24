import dlt
import pandas as pd
import requests
from datetime import date, timedelta


def get_exchange_rates(
    currency: str,
    start_date: date = date(2011, 1, 1),
    end_date: date = date(2014, 7, 1),
) -> pd.DataFrame:

    chunk_size = 365
    records = []

    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_size - 1), end_date)
        url = (
            f"http://api.nbp.pl/api/exchangerates/rates/a/{currency}"
            f"/{current_start}/{current_end}/"
        )
        response = requests.get(url, headers={"Accept": "application/json"})

        if response.status_code == 200:
            for entry in response.json()["rates"]:
                records.append({
                    "date": entry["effectiveDate"],
                    "currency": currency.upper(),
                    "rate": entry["mid"],
                })
        elif response.status_code != 404:
            response.raise_for_status()

        current_start = current_end + timedelta(days=1)

    df = pd.DataFrame(records, columns=["date", "currency", "rate"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def run_exchange_rate_pipeline():
    currency = "USD"

    df = get_exchange_rates(currency)
    print(f"Fetched {len(df)} records for {currency}")

    pipeline = dlt.pipeline(
        pipeline_name="exchange_rate_pipeline",
        destination="mssql",
        dataset_name="Extract",
    )

    info = pipeline.run(
        df,
        table_name="CurrencyRateData",
        write_disposition="replace",
    )
    print(info)


if __name__ == "__main__":
    run_exchange_rate_pipeline()