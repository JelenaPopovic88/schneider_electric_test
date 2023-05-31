import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

spark = SparkSession.builder \
                    .appName('PySpark SE') \
                    .getOrCreate()
# Read data
df = (spark.read
      .option('multiple', 'true')
      .json('/home/jelena/PycharmProjects/schneider_electric_test/new_file.json')
      .distinct()
      )
# Select columns of interests
df = (df.select(f.col('created_at').cast(TimestampType()),
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

### Repository aggregation
w_count_usr = Window.partitionBy('created_at_date', 'repo_id', 'repo_name', 'type')
df_repository = (df
                 .withColumn('num_user_event', f.sum(f.col('event_rank')).over(w_count_usr))
                 .drop(f.col('actor_id'), f.col('actor_login'), f.col('event_rank'))
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

# Write repository aggregations
df_repository.write.csv('/home/jelena/PycharmProjects/schneider_electric_test/agg_repo.csv', header=True, mode='overwrite')

### User aggregation
w_count_repo = Window.partitionBy('created_at_date', 'actor_id', 'actor_login', 'type')
df_user = (df
                .filter(f.col('type') != 'ForkEvent')
                .withColumn('num_repo_event', f.sum(f.col('event_rank')).over(w_count_repo))
                .drop(f.col('repo_id'), f.col('repo_name'), f.col('event_rank'), f.col('payload_action'))
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

# Write user aggregation
df_user.write.csv('/home/jelena/PycharmProjects/schneider_electric_test/agg_user.csv', header=True, mode='overwrite')


spark.stop()
