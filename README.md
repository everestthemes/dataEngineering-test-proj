 Given a set of daily taxi trips ( https://drive.google.com/file/d/1fpH3_g3V_aJdjyho52EraGPrEfI1GnMq/view?usp=share_link ), produce this following summary:

How many distinct car_type there are in the csv
How many distinct "geo" or geography there are in the dataset
For every day in 2017 i.e from 2017-01-10 to 2017-12-31 Count the number of trips taken by each (car_type, geo) combination.  If there isn't a trip taken by an existing (car_type, geo) combination during a particular day, show 0. 


Solutions:

Prerequisites required

1. PostgresDB server installed
2. Python 3.x installed
3. Python library installed: pip3, pandas, psycopg2, sqlalchemy
4. Download the dataset from https://drive.google.com/file/d/1fpH3_g3V_aJdjyho52EraGPrEfI1GnMq/view?usp=share_link
5. Create DB (eg. datalake)

Modify the script as per your own Postgres DB Host, Username,password, DB name, port

To run the script:
     python3 project-dataEngineering.py


After running the script you will get 3 csv output file as per the Questions above.
