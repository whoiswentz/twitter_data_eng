from airflow.utils.dates import days_ago
from datetime import datetime
from os.path import join

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from typing import Any, Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pathlib import Path
import json

import json
from airflow.utils.helpers import partition
import requests
from typing import Any, Generator, Optional

from airflow.hooks.http_hook import HttpHook
from requests.sessions import Session

from spark.transformation import twitter_transform


class TwitterHook(HttpHook):
    def __init__(
            self,
            query: str,
            conn_id: Optional[str] = None,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None):

        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(http_conn_id=self.conn_id)

    def paginate(self, url, session: Session, next_token: str = "") -> Generator[Any, None, None]:
        full_url = url
        if next_token:
            full_url = f"{url}&next_token={next_token}"
        data = self.connect_to_endpoint(full_url, session)
        yield data
        if "next_token" in data.get("meta", {}):
            yield from self.paginate(url, session, data['meta']['next_token'])

    def run(self):
        session = self.get_conn()
        url = self.create_url()

        yield from self.paginate(url, session)

    def create_url(self) -> str:
        query = self.query
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        start_time = (
            f"start_time={self.start_time}" if self.start_time else "")
        end_time = (f"end_time={self.end_time}" if self.end_time else "")
        url = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&{start_time}&{end_time}"
        return url

    def connect_to_endpoint(self, url: str, session: Session) -> Any:
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {}).json()


class TwitterOperator(BaseOperator):
    template_fields = ["query", "file_path",
                       "conn_id", "start_time", "end_time"]

    @apply_defaults
    def __init__(
        self,
        query: str,
        file_path: str,
        conn_id: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.query = query
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self) -> None:
        self.log.info(f"PATH: {self.file_path}")
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context: Any):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )

        self.create_parent_folder()
        with open(self.file_path, 'w') as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")


args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "twitter_data_eng/twitter_datalake/",
    "{stage}/twitter/{partition}"
)

PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
        dag_id="twitter_dag",
        default_args=args,
        schedule_interval="0 9 * * *",
        max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="get_tweets",
        query="TWITTER_USER",
        start_time=(
            "{{"
            f"execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
        ),
        end_time=(
            "{{"
            f"next_execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
        ),
        file_path=join(
            BASE_FOLDER.format(state="bronze", partition=PARTITION_FOLDER),
            "tweets_{{ ds_nodash }}.json"
        )
    )

    spark_operator = SparkSubmitOperator(
        task_id="transform_twitter",
        name="twitter_transformation",
        application=join(str(Path(__file__).parents[2]), "spark/transformation.py"),
        application_args=(
            "--source", BASE_FOLDER.format(state="bronze", partition=PARTITION_FOLDER),
            "--destination", BASE_FOLDER.format(state="silver", partition=""),
            "--process-date", "{{ ds }}"
        )
    )

    twitter_operator >> twitter_transform
