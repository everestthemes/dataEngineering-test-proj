import os
import pandas as pd
import psycopg2
import psycopg2.extras
import time
from sqlalchemy import create_engine,text

def db_conn(hostname,database,username,password,port):
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = password,
            port = port
        )
        print("Successfully connected to DB")
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return conn,cur
    except Exception as error:
        print(f"Failed to connect to database ... {error}")
        exit(1)

def csv_to_db(dataset_path,conn,cur):
        if os.path.exists(dataset_path):
            df = pd.read_csv(dataset_path,parse_dates = ['date'])
        else:
             print(f"File path {dataset_path} does not exist.")
             exit(1)
        cur.execute("DROP TABLE IF EXISTS daily_trips_by_geography")
        create_script = '''
            CREATE TABLE daily_trips_by_geography (car_type varchar,
            date DATE, 
            geo varchar, 
            trips float, 
            grouping varchar,
            monthly float,
            parent_type varchar,
            monthly_is_estimated BOOL
            )
        '''
        cur.execute(create_script)
        
        df.to_csv(dataset_path,header=df.columns,index=False, encoding='utf-8')
        file = open(dataset_path)
        sql_copy_script = '''
            COPY daily_trips_by_geography FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
        '''
        dbcopy = cur.copy_expert(sql=sql_copy_script, file=file)
        cur.execute('grant select on  table daily_trips_by_geography to public')
        conn.commit()

        if cur is not None:
            cur.close()
            print("Successfully Closed curser")
        if conn is not None:
            conn.close()
            print("Successfully Closed Connections")

def sqlalchemy_conn(hostname,database,username,password,port):
       dbstring = "postgresql://%s:%s@%s:%s/%s"%(username,password,hostname,port,database)
       sqlalchemy_conn = create_engine(dbstring).connect()
       return sqlalchemy_conn


def sqlQuery_out(files_path,car_type_resultFileName,geography_resultFileName,eachday_resultFileName,query_conn):

    if os.path.exists(files_path):
        ### READ data from table ######
        read_script_car_type = '''
            select DISTINCT car_type, COUNT(car_type) as count_distinct_total_car from daily_trips_by_geography GROUP BY car_type;
        '''
        read_script_geo = '''
            select DISTINCT geo, COUNT(geo) as count_distinct_total_geo from daily_trips_by_geography GROUP BY geo;
        '''
        read_script_eachday = '''
            select date,car_type, geo, SUM(trips) as total_trips from daily_trips_by_geography where date BETWEEN '2017-01-10' AND '2017-12-31' GROUP BY date,car_type, geo ORDER BY date;
        '''
        df1 = pd.read_sql(text(read_script_car_type),query_conn)
        df2 = pd.read_sql(text(read_script_geo),query_conn)
        df3 = pd.read_sql(text(read_script_eachday),query_conn)
        # Clean
        df1 = df1.dropna(axis=0)
        df2 = df2.dropna(axis=0)
        df3 = df3.dropna(axis=0)
        # export
        df1.to_csv(os.path.join(files_path, car_type_resultFileName), index=False)
        df2.to_csv(os.path.join(files_path, geography_resultFileName), index=False)
        df3.to_csv(os.path.join(files_path, eachday_resultFileName), index=False)

        if query_conn is not None:
            query_conn.close()
            print("Successfully Closed Connections")
    else:
        print(f"File path {files_path} does not exist.")
        exit(1)

def main():
    hostname = '10.10.1.10'
    database = 'datalake'
    username = 'bhuone'
    password = 'eX8787bhhgVYGYgCuKJDH9uB*A+U2'
    port = 5432
    conn = None
    cur = None
    dataset_filename = "daily_trips_by_geography.csv"
    files_path = '/Users/bhuwanchaudhary/Downloads/' # Your CSV Dataset path 
    dataset_fullpath = "{}/{}".format(files_path,dataset_filename)
    timestr = time.strftime("%Y-%m-%d-%-H%M%S")
    car_type_resultFileName ="%s/Car_typeAnalysedData-%s.csv" %(files_path,timestr)
    geography_resultFileName ="%s/Geography_AnalysedData-%s.csv" %(files_path,timestr)
    eachday_resultFileName ="%s/Eachday_trips_AnalysedData-%s.csv" %(files_path,timestr)

    conn,cur = db_conn(hostname,database,username,password,port)
    csv_to_db(dataset_fullpath,conn,cur)
    query_conn = sqlalchemy_conn(hostname,database,username,password,port)
    sqlQuery_out(files_path,car_type_resultFileName,geography_resultFileName,eachday_resultFileName,query_conn)

if __name__ == '__main__':
    main()

