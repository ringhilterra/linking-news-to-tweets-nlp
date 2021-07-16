import click
from workflow import *

CONTEXT_SETTINGS = {'auto_envvar_prefix': '****'}

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--news-table", default=None,
              help="news table")
@click.option("--news-date", default=None,
              help="Unique simulation run id")
@click.option("--tweet-table", default=None,
              help="Simulation template directory")
def main(news_table, news_date, tweet_table):
    print(news_table, news_date, tweet_table)
    result_df = run_main(news_table, news_date, tweet_table)


if __name__ == '__main__':
    main()
