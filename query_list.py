from pyspark.sql import SparkSession
import pyspark
import datetime

spark = SparkSession.builder.appName('Stock Project').getOrCreate()
spark_df = spark.read.csv('csv_database/*.csv', sep=',', header=True)
spark_df = spark_df.withColumn('Date', spark_df['Date'].cast('date'))
spark_df.createTempView('stocks')
print(spark_df.count())


# Query 1
def get_max_diff_stock():
    try:
        query = """
         select stocks.company, stocks.date, stocks.max_diff_stock_percent from (Select date,
         company,(high-open)/open*100 as max_diff_stock_percent, dense_rank() 
         OVER (partition by date order by ( high-open)/open desc ) as dense_rank FROM stock_table)stocks where
         stocks.dense_rank=1
      """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date']] = {'company': row['company'], 'max_diff_stock_percent': row['max_diff_stock_percent']}
        return results
    except Exception as e:
        return {'Error': e}


# Query 2
def get_most_traded_stock():
    try:
        query = """
         select stock_table.company, stock_table.date, stock_table.volume from (Select date, company, volume,
         dense_rank() over (partition by date order by volume desc) as dense_rank from stocks)stock_table
         where stock_table.dense_rank=1
           """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date'].strftime("%Y-%m-%d")] = {'company': row['company'], 'date': row['date'],
                                                         'volume': row['volume']}
        return results
    except Exception as e:
        return {'Error': e}


# Query 3
def get_max_gap():
    try:
        query = """
         select stock_table.company,abs(stock_table.previous_close-stock_table.open) as max_gap from 
         (Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by date) as
         previous_close from stocks asc)stock_table order by max_gap desc limit 1
       """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['max_gap'] = row['max_gap']
        return results
    except Exception as e:
        return {'Error': e}


# Query 4
def get_max_moved_stock():
        try:
            query = """
            with df1 as (select company, open from (select company, open, dense_rank() over (partition by company order by date) as d_rank1 from stocks)stock_table where stock_table.d_rank1=1)
              , df2 as (select company, close from (select company, close, dense_rank() over (partition by company order by date desc) as d_rank2 from stocks)stock_table2 where stock_table2.d_rank2 = 1)
              select df1.company, df1.open, df2.close, df1.open-df2.close as max_diff from df1 inner join df2 where df1.company = df2.company
              order by max_diff DESC limit 1
            """
            data = spark.sql(query).collect()
            results = {}
            for row in data:
                results['company'] = row['company']
                results['open'] = row['open']
                results['close'] = row['close']
                results['max_diff'] = row['max_diff']
            return results
        except Exception as e:
            return {'Error': e}


# Query 5
def get_standard_devaition():
    try:
        query = """
           select Company, stddev_samp(Volume) as Standard_Deviation from stocks group by Company
       """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Standard_Deviation': val})
        return results
    except Exception as e:
        return {'Error': e}


# Query 6
def get_stock_mean_and_median():
    try:
        query = """ 
            select company, avg(Close) as mean, percentile_approx(Close,0.5) as median from stocks group by company
       """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append({'company': row['company'], 'mean': row['mean'], 'median': row['median']})
        return results
    except Exception as e:
        return {'Error': e}


# Query 7
def get_avg_volume():
    try:
        query = """
           select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc
       """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average volume': val})
        return results
    except Exception as e:
        return {'Error': e}


# Query 8
def get_highest_avg_volume():
    try:
        query = """
           select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc limit 1
       """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Highest average volume': val})
        return results
    except Exception as e:
        return {'Error': e}


# Query 9
def get_highest_and_lowest_stock_price():
    try:
        query = """
           select Company, MAX(high) as Highest_Price, MIN(low) as Lowest_Price from stocks group by Company
       """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append(
                {'company': row['Company'], 'highest_price': row['Highest_Price'], 'lowest_price': row['Lowest_Price']})
        return results
    except Exception as e:
        return {'Error': e}
