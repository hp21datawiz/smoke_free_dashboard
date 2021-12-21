from google.cloud import bigquery
import logging
import google.cloud.logging
import datetime
import pandas as pd
import os
import re
import requests as r


config = {
    "APPFIGURES_USERNAME": "accounts@flipsidegroup.com",
    "APPFIGURES_PASSWORD": "Waterhouse123!",
    "APPFIGURES_APP_KEY": "a3366e1195214ac499a53c2985e11efa",
}

BASE_URI = "https://api.appfigures.com/v2/"

af_username = config["APPFIGURES_USERNAME"]
af_password = config["APPFIGURES_PASSWORD"]
af_app_key = config["APPFIGURES_APP_KEY"]


def make_request(uri, **querystring_params):
    headers = {"X-Client-Key": af_app_key}
    auth = (af_username, af_password)
    return r.get(
        BASE_URI + uri.lstrip("/"),
        auth=auth,
        params=querystring_params,
        headers=headers,
    )


# Instantiates a client
client = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
client.get_default_handler()
client.setup_logging()

cloud_logger = client.logger("cloudLogger")

# make bigquery client
client = bigquery.Client()


# get yesterday date as string
yesterday_dt = datetime.datetime.now().date() - datetime.timedelta(days=1)
yesterday_string = yesterday_dt.strftime("%Y%m%d")


def main(data, context):

    cloud_logger.log_text("Starting script")

    prod_id_android = "33301721834"
    prod_id_ios = "33151374316"
    prod_id_combined = "33301721834,33151374316"

    cloud_logger.log_text("Making request")
    root_response = make_request("/")
    assert 200 == root_response.status_code
    assert af_username == root_response.json()["user"]["email"]

    prod_ids = [
        {"name": "android", "id": prod_id_android},
        {"name": "ios", "id": prod_id_ios},
    ]
    df = pd.DataFrame()
    for prod_id in prod_ids:
        cloud_logger.log_text("Requesting {}".format(prod_id["name"]))
        resp = make_request(
            "/reports/sales",
            start_date="2021-09-21",
            products=prod_id["id"],
            group_by="products,dates",
        )
        json = resp.json()
        my_data = json[prod_id["id"]]
        _df = pd.DataFrame(my_data).transpose()
        _df["operating_system"] = prod_id["name"]
        df = df.append(_df)

    df = df[["date", "operating_system", "downloads", "uninstalls"]].reset_index(
        drop=True
    )

    project_id = "smokefree-4014f"
    dataset_id = "smoke_free_eu"
    table_name = "appfigures_sales"
    table_id = project_id + "." + dataset_id + "." + table_name

    cloud_logger.log_text("Uploading data")

    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("operating_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("downloads", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("uninstalls", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("operating_system", "STRING"),
            bigquery.SchemaField("downloads", "INT64"),
            bigquery.SchemaField("uninstalls", "INT64"),
        ]
    )

    job_config.write_disposition = "WRITE_TRUNCATE"

    table_id = dataset_id + "." + table_name

    df["date"] = pd.to_datetime(df["date"])
    df["downloads"] = pd.to_numeric(df["downloads"])
    df["uninstalls"] = pd.to_numeric(df["uninstalls"])

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config, project=project_id
    )

    # Wait for the load job to complete.
    job.result()

    count = 500
    root_response = make_request("/")
    assert 200 == root_response.status_code
    assert af_username == root_response.json()["user"]["email"]

    prod_ids = [
        {"name": "android", "id": prod_id_android},
        {"name": "ios", "id": prod_id_ios},
    ]
    prod_dict = {"33301721834": "android", "33151374316": "ios"}
    # app figures ratings
    resp = make_request(
        "/reports/ratings/",
        start_date="2021-09-21",
        count=count,
        products=prod_id_combined,
        group_by="date,product",
    )
    data_json = resp.json()
    adf = pd.DataFrame()
    for date in data_json.keys():
        for prod in prod_dict.keys():
            for i in range(5):
                stars = i + 1
                value = data_json[date][prod]["new"][i]
                adf = adf.append(
                    pd.DataFrame(
                        {
                            "date": date,
                            "platform": prod_dict[prod],
                            "stars": stars,
                            "count": value,
                        },
                        index=[0],
                    )
                )
                adf = adf.reset_index(drop=True)

    table_name = "appfig_ratings_by_day"
    table_id = project_id + "." + dataset_id + "." + table_name

    adf = adf.rename({"platform": "OS"}, axis=1)

    #
    cloud_logger.log_text("Writing ratings by day...")

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("OS", "STRING"),
            bigquery.SchemaField("stars", "INT64"),
            bigquery.SchemaField("count", "INT64"),
        ]
    )

    job_config.write_disposition = "WRITE_TRUNCATE"

    table_id = dataset_id + "." + table_name

    adf["date"] = pd.to_datetime(adf["date"])
    adf["stars"] = pd.to_numeric(adf["stars"])
    adf["count"] = pd.to_numeric(adf["count"])

    job = client.load_table_from_dataframe(
        adf, table_id, job_config=job_config, project=project_id
    )

    job.result()
    cloud_logger.log_text("Ratings by day complete.")


if __name__ == "__main__":

    main("data", "context")

