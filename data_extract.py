from dotenv import load_dotenv
load_dotenv()

import dlt
from dlt.sources.sql_database import sql_database
from dlt.sources.filesystem import filesystem, read_csv
from exchange_rates import run_exchange_rate_pipeline


def run_csv_pipeline(file_glob="SBI2526-LAB-Rating-FixedDate.csv"):
    csv_source = filesystem(
        bucket_url=".",
        file_glob=file_glob
    ) | read_csv()

    pipeline = dlt.pipeline(
        pipeline_name="aw_csv_pipeline",
        destination="mssql",
        dataset_name="Extract"
    )

    info = pipeline.run(csv_source, table_name="ProductRating", write_disposition="replace")
    print(info)


def run_pipeline():

    production = sql_database(schema='Production').with_resources(
        'Product',
        'ProductSubcategory',
        'ProductCategory'
        )

    sales = sql_database(schema='Sales').with_resources(
        'SalesPerson',
        'SalesTerritory',
        'SalesOrderHeader',
        'SalesOrderDetail'
        )

    person = sql_database(schema='Person').with_resources(
        'Person',
        'CountryRegion'
        )

    
    pipeline = dlt.pipeline(
        pipeline_name="aw_pipeline",
        destination="mssql",
        dataset_name="Extract"   
    )

    
    info = pipeline.run([production,sales,person], write_disposition="replace")
    print(info)



if __name__ == "__main__":
    run_pipeline()
    run_csv_pipeline()
    run_exchange_rate_pipeline()