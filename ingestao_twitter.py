import tweepy
import datetime
from google.cloud import bigquery
from airflow import models
from airflow.operators import python_operator

client = bigquery.Client(location='southamerica-east1')

default_dag_args = {
    'start_date': datetime.datetime(2022, 11, 25),
}

def initialize_twitter_api():
    client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAEvyjgEAAAAA6rtwgSkmnlrFFzrlseNt6N8OQGU%3DwGV9EMjKWbJjwyW1sGwKC5jfZAB6UaBAFT8oRvJ5VXLxMz2wC0')
    return client


def fetch_most_sold_line(year, month):
    query = """
        SELECT linha
        FROM `trusted.tb_venda_linha_anomes`
        WHERE ano = {year} AND mes = {month}
        ORDER BY total_venda
        DESC LIMIT 1
    """.format(year=year, month=month)
    results = client.query(query, location='southamerica-east1')
    for result in results:
        return result['linha']


with models.DAG(
        'ingestao_tb_tweets',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    def runner():
        table_id = 'delivery.tb_tweets'
        table = client.get_table(table_id)
        api = initialize_twitter_api()
        most_sold_line = fetch_most_sold_line(2019, 2)
        print(most_sold_line)
        query = 'Boticário ' + most_sold_line

        tweets = api.search_recent_tweets(query=query, tweet_fields='lang', max_results=100, user_fields=['name', 'username'], expansions='author_id')
        users = {u["id"]: u for u in tweets.includes['users']}
        rows_to_insert = []
        while len(rows_to_insert) < 50:
            for tweet in tweets.data:
                if tweet.lang != 'pt':
                    continue
                if users[tweet.author_id]:
                    user = users[tweet.author_id]
                    rows_to_insert.append({'username': user.username, 'name': user.name, 'text': tweet.text})
                if rows_to_insert == 50:
                    break
            if len(rows_to_insert) == 50:
                break
            if 'meta' in tweets and 'next_token' in tweets.meta:
                tweets = api.search_recent_tweets(query=query, tweet_fields='lang', max_results=100,
                                                  next_token=tweets.meta.next_token,
                                                  user_fields=['name', 'username'], expansions='author_id')
                users = {u["id"]: u for u in tweets.includes['users']}
            else:
                break

        client.insert_rows(table, rows_to_insert)

    populate_table = python_operator.PythonOperator(
        task_id='ingestao_tb_tweets',
        python_callable=runner)