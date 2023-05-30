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
                f.col('repo.id').alias('repo_id'),
                f.col('repo.name').alias('repo_name'))
      )

# Filter events and rank events in order to count distinct users (for cases when same user stars and unstars repo)
events = ['WatchEvent', 'ForkEvent', 'IssuesEvent', 'PullRequestEvent']
actions = ['opened', 'started']
w_rank = Window.partitionBy('created_at', 'repo_id', 'repo_name', 'type', 'payload_action').orderBy('created_at')
df_repository = (df.filter(f.col('type').isin(events))
                 .filter(f.col('payload_action').isin(actions) | f.col('payload_action').isNull())
                 .withColumn('event_rank', f.dense_rank().over(w_rank))
                 .filter(f.col('event_rank')==1)
                 .drop(f.col('created_at'), f.col('event_rank'))
                 )

# Repository aggregation
w_count = Window.partitionBy('created_at_date', 'repo_id', 'repo_name', 'type', 'payload_action')
df_repository = (df_repository
                 .withColumn('num_user_event', f.count(f.col('actor_id')).over(w_count))
                 .drop(f.col('actor_id'))
                 .distinct()
                 )

df_repository = (df_repository
                 .withColumn('num_user_star',
                                        f.when((f.col('type')=='WatchEvent') & (f.col('payload_action')=='started'),
                                                 f.col('num_user_event')))
                 .withColumn('num_user_forked', f.when(f.col('type')=='ForkEvent', f.col('num_user_event')))
                 .withColumn('num_user_created_issues',
                                        f.when((f.col('type')=='IssuesEvent') & (f.col('payload_action')=='opened'),
                                                 f.col('num_user_event')))
                 .withColumn('num_user_created_pr',
                             f.when((f.col('type') == 'PullRequestEvent') & (f.col('payload_action') == 'opened'),
                                    f.col('num_user_event')))

                 .drop(f.col('payload_action'))
                 )
# df_repository.select('type', 'payload_action').distinct().show()


df_repository.show(10, False)
# df.printSchema()

#

spark.stop()
