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
        .withColumn('repo_name_new', f.last(f.col('repo_name'), ignorenulls=True).over(w_u))
      )

### TO REMOVE
# df.select('repo_name', 'repo_id').groupBy('repo_id').agg(f.countDistinct('repo_name').alias('test')).orderBy(f.col('test').desc()).show()
# df.filter(f.col('repo_id')==28688784).select('created_at','actor_id', 'actor_login', 'repo_id', 'repo_name', 'repo_name_new').orderBy('created_at').show()
# df.show()
# df.filter(f.col('actor_id')==6078139).show()
### TO REMOVE

# Filter events and rank events in order to count distinct users (for cases when same user stars and unstars repo)
events = ['WatchEvent', 'ForkEvent', 'IssuesEvent', 'PullRequestEvent']
actions = ['opened', 'started']
w_rank = Window.partitionBy('created_at_date','actor_id', 'repo_id', 'type', 'payload_action').orderBy('created_at')
df = (df.filter(f.col('type').isin(events))
                 .filter(f.col('payload_action').isin(actions) | f.col('payload_action').isNull())
                 .withColumn('event_rank', f.dense_rank().over(w_rank))
                 .filter(f.col('event_rank')==1)
                 .drop(f.col('created_at'), f.col('event_rank'))
                 )


### Repository aggregation
w_count = Window.partitionBy('created_at_date', 'repo_id', 'repo_name', 'type', 'payload_action')
df_repository = (df
                 .withColumn('num_user_event', f.count(f.col('actor_id')).over(w_count))
                 .drop(f.col('actor_id'))
                 .distinct()
                 )
# Pivot table and rename columns
df_repository = (df_repository.groupBy('created_at_date', 'repo_id', 'repo_name')
                 .pivot('type').sum('num_user_event')
                 )
df_repository = (df_repository.withColumnRenamed('WatchEvent', 'num_user_star')
                               .withColumnRenamed('ForkEvent', 'num_user_forked')
                               .withColumnRenamed('IssuesEvent', 'num_user_created_issues')
                               .withColumnRenamed('PullRequestEvent', 'num_user_created_pr')
                 )

# Write repository aggregations
df_repository.show()

# Check calculation
# df_repository.filter(f.col('repo_name')=='conda/conda').show()
# df_repository.show(10, False)
# df_repository.filter(f.col('repo_name')=='ttezel/twit').show()

### User aggregation


# df_repository = (df_repository
#                  .withColumn('num_user_star',
#                                         f.when((f.col('type')=='WatchEvent') & (f.col('payload_action')=='started'),
#                                                  f.col('num_user_event')))
#                  .withColumn('num_user_forked', f.when(f.col('type')=='ForkEvent', f.col('num_user_event')))
#                  .withColumn('num_user_created_issues',
#                                         f.when((f.col('type')=='IssuesEvent') & (f.col('payload_action')=='opened'),
#                                                  f.col('num_user_event')))
#                  .withColumn('num_user_created_pr',
#                              f.when((f.col('type') == 'PullRequestEvent') & (f.col('payload_action') == 'opened'),
#                                     f.col('num_user_event')))
#
#                  .drop(f.col('payload_action'))
#                  )
# df_repository.groupBy('repo_name').count().orderBy(f.col('count').desc()).show()

# df.printSchema()

#

spark.stop()
