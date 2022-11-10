from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
                        .appName("homework_2") \
                        .getOrCreate()

    sc = spark.sparkContext
    input_path = 'flights.csv'

    df = spark.read.option("delimiter", ",").option("header", True).csv(input_path)

    df = df \
        .withColumn("DEPARTURE_DELAY", df["DEPARTURE_DELAY"].cast('int')) \
        .withColumn("CANCELLED", df["CANCELLED"].cast('int')) \

    # 1 Origin Airports
    origin_airports = df.select('ORIGIN_AIRPORT').distinct().rdd.flatMap(list).collect()

    with open(r'output_1.txt', 'w') as f:
        for o in origin_airports:
            f.write("%s\n" % o)

    # 2 Origin Destination pairs
    origin_destination = df.select('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT').distinct().rdd.map(list).collect()

    with open(r'output_2.txt', 'w') as f:
        for pair in origin_destination:
            f.write("(%s, %s)\n" % (pair[0], pair[1]))

    # 3 Origin Airport with the largest departure delay in January
    max_dep_delay_in_jan = df.where(df.MONTH == '1').select(F.max(F.col('DEPARTURE_DELAY'))).collect()[0]['max(DEPARTURE_DELAY)']
    origin_with_largest_delay_jan = df.filter((F.col('DEPARTURE_DELAY') == max_dep_delay_in_jan)).collect()[0]['ORIGIN_AIRPORT']

    with open(r'output_3.txt', 'w') as f:
        f.write(origin_with_largest_delay_jan)

    # 4 Carier (airline) with the largest delay on Weekends
    max_dep_del_on_weekends = df.where((df.DAY_OF_WEEK == '6') | (df.DAY_OF_WEEK == '7')).select(F.max(F.col('DEPARTURE_DELAY'))).collect()[0]['max(DEPARTURE_DELAY)']
    airline_with_largest_delay_weekends = df.filter((F.col('DEPARTURE_DELAY') == max_dep_del_on_weekends)).collect()[0]['AIRLINE']

    with open(r'output_4.txt', 'w') as f:
        f.write(airline_with_largest_delay_weekends)

    # 5 Airport with the most cancellation of flights
    df_cancelled = df.select('ORIGIN_AIRPORT', 'CANCELLED').where(df.CANCELLED == '1').groupby('ORIGIN_AIRPORT').sum('CANCELLED')
    df_cancelled = df_cancelled.withColumnRenamed("sum(CANCELLED)", "NUMBER_OF_CANCELATIONS")
    max_num_of_cancellation = df_cancelled.select(F.max(F.col('NUMBER_OF_CANCELATIONS'))).collect()[0]['max(NUMBER_OF_CANCELATIONS)']
    airport_with_most_cancellation = df_cancelled.filter((F.col('NUMBER_OF_CANCELATIONS') == max_num_of_cancellation)).collect()[0]['ORIGIN_AIRPORT']

    with open(r'output_5.txt', 'w') as f:
        f.write(airport_with_most_cancellation)

    # 6 Percentage of cancelled flights for each airline
    df_cancelled_info = df.groupby('AIRLINE').agg(F.sum('CANCELLED'), F.count('CANCELLED')).withColumnRenamed('sum(CANCELLED)', 'NUMBER_OF_CANCELATIONS') \
                                                                                           .withColumnRenamed('count(CANCELLED)', 'TOTAL')
    df_cancelled_info = df_cancelled_info.withColumn('PERCENTAGE', F.round((df_cancelled_info.NUMBER_OF_CANCELATIONS/df_cancelled_info.TOTAL)*100, 2))
    percentage_of_cancellation_per_airline = df_cancelled_info.select('AIRLINE', 'PERCENTAGE').rdd.map(list).collect()
    
    with open(r'output_6.txt', 'w') as f:
        for pair in percentage_of_cancellation_per_airline:
            f.write("(%s, %.2f%%)\n" % (pair[0], pair[1]))

    # 7 Largest departure delays for each airline
    largest_dep_delay_per_airline = df.groupby('AIRLINE').agg(F.max('DEPARTURE_DELAY')).rdd.map(list).collect()

    with open(r'output_7.txt', 'w') as f:
        for pair in largest_dep_delay_per_airline:
            f.write("(%s, %i)\n" % (pair[0], pair[1]))

    # 8 Largest departure delays for each airline in each month
    largest_dep_delay_per_airline_each_month = df.groupby('MONTH', 'AIRLINE').agg(F.max('DEPARTURE_DELAY')).rdd.map(list).collect()

    with open(r'output_8.txt', 'w') as f:
        for row in largest_dep_delay_per_airline_each_month:
            if row[0] == '1':
                f.write("(%s, %i) in Jan\n" % (row[1], row[2]))
            elif row[0] == '2':
                f.write("(%s, %i) in Feb\n" % (row[1], row[2]))
            elif row[0] == '3':
                f.write("(%s, %i) in Mar\n" % (row[1], row[2]))
            elif row[0] == '4':
                f.write("(%s, %i) in Apr\n" % (row[1], row[2]))
            elif row[0] == '5':
                f.write("(%s, %i) in May\n" % (row[1], row[2]))
            elif row[0] == '6':
                f.write("(%s, %i) in Jun\n" % (row[1], row[2]))
            elif row[0] == '7':
                f.write("(%s, %i) in Jul\n" % (row[1], row[2]))
            elif row[0] == '8':
                f.write("(%s, %i) in Aug\n" % (row[1], row[2]))
            elif row[0] == '9':
                f.write("(%s, %i) in Sep\n" % (row[1], row[2]))
            elif row[0] == '10':
                f.write("(%s, %i) in Oct\n" % (row[1], row[2]))
            elif row[0] == '11':
                f.write("(%s, %i) in Nov\n" % (row[1], row[2]))
            elif row[0] == '12':
                f.write("(%s, %i) in Dec\n" % (row[1], row[2])) 

    # 9 Average departure delay for each airline
    avg_dep_delay_per_airline = df.groupby('AIRLINE').agg(F.round(F.avg('DEPARTURE_DELAY'), 2)).rdd.map(list).collect()

    with open(r'output_9.txt', 'w') as f:
        for pair in avg_dep_delay_per_airline:
            f.write("(%s, %.2f)\n" % (pair[0], pair[1]))

    # 10 Average departure delay for each airline in each month
    avg_dep_delay_per_airline_each_month = df.groupby('MONTH', 'AIRLINE').agg(F.round(F.avg('DEPARTURE_DELAY'), 2)).rdd.map(list).collect()

    with open(r'output_10.txt', 'w') as f:
        for row in avg_dep_delay_per_airline_each_month:
            if row[0] == '1':
                f.write("(%s, %.2f) in Jan\n" % (row[1], row[2]))
            elif row[0] == '2':
                f.write("(%s, %.2f) in Feb\n" % (row[1], row[2]))
            elif row[0] == '3':
                f.write("(%s, %.2f) in Mar\n" % (row[1], row[2]))
            elif row[0] == '4':
                f.write("(%s, %.2f) in Apr\n" % (row[1], row[2]))
            elif row[0] == '5':
                f.write("(%s, %.2f) in May\n" % (row[1], row[2]))
            elif row[0] == '6':
                f.write("(%s, %.2f) in Jun\n" % (row[1], row[2]))
            elif row[0] == '7':
                f.write("(%s, %.2f) in Jul\n" % (row[1], row[2]))
            elif row[0] == '8':
                f.write("(%s, %.2f) in Aug\n" % (row[1], row[2]))
            elif row[0] == '9':
                f.write("(%s, %.2f) in Sep\n" % (row[1], row[2]))
            elif row[0] == '10':
                f.write("(%s, %.2f) in Oct\n" % (row[1], row[2]))
            elif row[0] == '11':
                f.write("(%s, %.2f) in Nov\n" % (row[1], row[2]))
            elif row[0] == '12':
                f.write("(%s, %.2f) in Dec\n" % (row[1], row[2]))

if __name__ == "__main__":
    main()