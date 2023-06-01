# schneider_electric_test
This repository contains two scripts:
1. download_data.py
2. repository_aggregation.py

- repository_aggregation.py script downloads the files, reads and cleans data, and calculates repository aggregation
and user aggregation. It expects two inputs YEAR and MONTH. These two inputs are harcoded in YEAR = 2015 and MONTH = 1. 
In case of running script for any other date, YEAR and MONTH should be changed inside the repository_aggregation.py script.
Results are written in two directories agg_repo.csv and agg_user.csv.

- download_data.py can be run separately, where user has option to input year and month.
