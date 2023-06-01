from download_data import download_month
import pyspark.sql as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# INPUT
YEAR = 2015
MONTH = 1

def clean_data(df) -> ps.DataFrame:
    """
    Method selects columns necessary for aggregation, unifies repo and usernames and filters events
    (WatchEvent, ForkEvent, IssueEvent and PullRequestEvent) and appropriate actions.
    Events are ranked in order to count distinct users and repos for starred and forked events.
    :param df:
    :return: dataframe
    """
    # Select columns of interests
    df = (df.withColumn('created_at', f.to_timestamp(f.unix_timestamp('created_at', "yyyy-MM-dd'T'HH:mm:ss'Z'")))
            .select('created_at',
                    f.col('created_at').cast(DateType()).alias('created_at_date'),
                    f.col('type'), f.col('payload.action').alias('payload_action'),
                    f.col('actor.id').alias('actor_id'),
                    f.col('actor.login').alias('actor_login'),
                    f.col('repo.id').alias('repo_id'),
                    f.col('repo.name').alias('repo_name'))
          )

    # Take last username and last repo name for the same user id and repo id
    w_u = Window().partitionBy('created_at_date', 'actor_id').orderBy('created_at').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    w_r = Window().partitionBy('created_at_date', 'repo_id').orderBy('created_at').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = (df.withColumn('actor_login', f.last(f.col('actor_login'), ignorenulls=True).over(w_u))
            .withColumn('repo_name', f.last(f.col('repo_name'), ignorenulls=True).over(w_r))
          )


    # Filter events and rank events in order to count distinct users (for cases when same user stars and unstars repo)
    events = ['WatchEvent', 'ForkEvent', 'IssuesEvent', 'PullRequestEvent']
    actions = ['opened', 'started']
    w_rank = Window.partitionBy('created_at_date','actor_id', 'repo_id', 'type', 'payload_action').orderBy('created_at')
    df = (df.filter(f.col('type').isin(events))
                     .filter(f.col('payload_action').isin(actions) | f.col('payload_action').isNull())
                     .withColumn('event_rank', f.dense_rank().over(w_rank))
                     .withColumn('event_rank',
                                 f.when((f.col('type').isin('WatchEvent', 'ForkEvent')) & (f.col('event_rank')>1), 0).otherwise(1))
                     .drop(f.col('created_at'))
                     )
    return df
def repo_agg(df):
    """
    Method calculates daily activities (Starred, Forked, Created issues and PRs) in the repo.
    :param df:
    """
    ### Repository aggregation
    w_count_r = Window.partitionBy('created_at_date', 'repo_id', 'repo_name', 'type')
    df_repository = (df
                     .withColumn('num_user_event', f.sum(f.col('event_rank')).over(w_count_r))
                     .drop('actor_id', 'actor_login', 'event_rank')
                     .distinct()
                     )

    # Pivot table and rename columns
    df_repository = (df_repository.groupBy('created_at_date', 'repo_id', 'repo_name')
                     .pivot('type').sum('num_user_event')
                     )
    df_repository = (df_repository.withColumnRenamed('WatchEvent', 'num_user_star')
                                   .withColumnRenamed('ForkEvent', 'num_user_forked')
                                   .withColumnRenamed('IssuesEvent', 'num_created_issues')
                                   .withColumnRenamed('PullRequestEvent', 'num_created_pr')
                     )
    # Fill null with 0
    df_repository = df_repository.fillna(0)

    # Write repository aggregations
    df_repository.write.csv('./agg_repo.csv', header=True, mode='overwrite')

def user_agg(df):
    """
    Method calculates daily user activity aggregates for starred projects, created issues and PRs.
    :param df:
    """
    w_count_u = Window.partitionBy('created_at_date', 'actor_id', 'actor_login', 'type')
    df_user = (df
                    .filter(f.col('type') != 'ForkEvent')
                    .withColumn('num_repo_event', f.sum(f.col('event_rank')).over(w_count_u))
                    .drop('repo_id', 'repo_name', 'event_rank', 'payload_action')
                    .distinct()
               )

    # Pivot table and rename columns
    df_user = (df_user.groupBy('created_at_date', 'actor_id', 'actor_login')
                     .pivot('type').sum('num_repo_event')
                     )

    df_user = (df_user.withColumnRenamed('WatchEvent', 'num_repo_star')
                                   .withColumnRenamed('IssuesEvent', 'num_created_issues')
                                   .withColumnRenamed('PullRequestEvent', 'num_created_pr')
                     )
    # Fill null with 0
    df_user = df_user.fillna(0)

    # Write user aggregation
    df_user.write.csv('./agg_user.csv', header=True, mode='overwrite')


if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName('PySpark SE')
             .getOrCreate()
             )
    download_month(YEAR, MONTH)
    # Read data
    df = (spark.read
          .option('multiple', 'true')
          .json('./downloads1/*.json')
          .distinct()
          )
    df_cleaned = clean_data(df)
    repo_agg(df_cleaned)
    user_agg(df_cleaned)
    spark.stop()
