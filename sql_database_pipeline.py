import dlt
from dlt.sources.sql_database import sql_database


def run_pipeline():

    source = sql_database(schema="Person").with_resources("Person")

    
    pipeline = dlt.pipeline(
        pipeline_name="person_pipeline",
        destination="mssql",
        dataset_name="raw"   
    )

    
    info = pipeline.run(source, write_disposition="replace")

    print(info)


if __name__ == "__main__":
    run_pipeline()