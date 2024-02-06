import pyspark.sql.types as T
from pyspark.sql.functions import when, col, count, expr, sum
from pyspark.sql import SparkSession


payment_schema = (
    T.StructType([
        T.StructField('payment_id', T.IntegerType(), True),
        T.StructField('customer_id', T.IntegerType(), True),
        T.StructField('staff_id', T.IntegerType(), True),
        T.StructField('rental_id', T.IntegerType(), True),
        T.StructField('amount', T.DoubleType(), True),
        T.StructField('payment_date', T.DateType(), True)
    ])
)

rental_schema = (
    T.StructType([
        T.StructField('rental_id', T.IntegerType(), True),
        T.StructField('rental_date', T.DateType(), True),
        T.StructField('inventory_id', T.IntegerType(), True),
        T.StructField('customer_id', T.IntegerType(), True),
        T.StructField('return_date', T.DateType(), True),
        T.StructField('staff_id', T.IntegerType(), True),
        T.StructField('last_update', T.DateType(), True),
    ])
)

spark = SparkSession.builder.appName('spark_app').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# CREATE DF
df_actor = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/actor.csv')
df_address = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/address.csv')
df_category = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/category.csv')
df_city = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/city.csv')
df_customer = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/customer.csv')
df_film = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/film.csv')
df_film_actor = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/film_actor.csv')
df_film_category = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/film_category.csv')
df_inventory = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/inventory.csv')
df_payment = spark.read.option('header', 'true').schema(payment_schema).csv('./csv_data/payment.csv')
df_rental = spark.read.option('header', 'true').schema(rental_schema).csv('./csv_data/rental.csv')
# df_payment = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/payment.csv')
# df_rental = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('./csv_data/rental.csv')

# DF TRANSFORMATIONS
print('1.Вывести количество фильмов в каждой категории, отсортировать по убыванию.')
df_j = df_category.join(df_film_category, 'category_id', 'left')
# df_j = df_category.join(df_film_category, df_category['category_id'] == df_film_category['category_id'], 'left')
df1 = (df_j
       .groupBy('name')
       .agg(count('name').alias('films_count'))
       .orderBy('films_count', ascending=False)
       )
df1.show()

print('2.Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.')
df_j = (df_actor
        .join(df_film_actor, df_actor['actor_id'] == df_film_actor['actor_id'])
        .join(df_inventory, df_film_actor['film_id'] == df_inventory['film_id'])
        .join(df_rental, df_inventory['inventory_id'] == df_rental['inventory_id'])
        )
df2 = (df_j
       .groupBy(['first_name', 'last_name'])
       .agg(count('*').alias('rental_count'))
       .orderBy('rental_count', ascending=False).limit(10)
       )
df2.show()

print('3.Вывести категорию фильмов, на которую потратили больше всего денег.')
df_j = (df_payment
        .join(df_rental, df_payment['rental_id'] == df_rental['rental_id'])
        .join(df_inventory, df_rental['inventory_id'] == df_inventory['inventory_id'])
        .join(df_film, df_inventory['film_id'] == df_film['film_id'])
        .join(df_film_category, df_film['film_id'] == df_film_category['film_id'])
        .join(df_category, df_film_category['category_id'] == df_category['category_id'])
        )
df3 = (df_j
       .groupBy('name')
       .agg(sum('amount').alias('total_sales'))
       .orderBy('total_sales', ascending=False)
       .limit(1)
      )
df3.show()

print('4.Вывести названия фильмов, которых нет в inventory.')
df_j = df_film.join(df_inventory, df_film['film_id'] == df_inventory['film_id'], 'leftanti')
# df_j = df_film.join(df_inventory, 'film_id', 'left').filter(df_inventory['film_id'].isNull())
df4 = df_j.select(df_film['title'])
df4.show(truncate=False)

print('5.Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.')
print('Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.')
df_j = (df_actor
        .join(df_film_actor, df_actor['actor_id'] == df_film_actor['actor_id'])
        .join(df_film, df_film_actor['film_id'] == df_film['film_id'])
        .join(df_film_category, df_film['film_id'] == df_film_category['film_id'])
        .join(df_category, df_film_category['category_id'] == df_category['category_id'])
        )
actor_film_count_children = (df_j
                             .filter(df_category['name'] == 'Children')
                             .groupBy('first_name', 'last_name', 'name')
                             .agg(count('*').alias('film_count'))
                             )
top3_actors = (actor_film_count_children
               .select('film_count')
               .orderBy('film_count', ascending=False)
               .limit(3)
               )
df5 = (actor_film_count_children
       .join(top3_actors, 'film_count')
       .select('first_name', 'last_name', 'film_count')
       .orderBy('film_count', ascending=False)
       )
df5.show()

print('-- 6.Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).')
print('-- Отсортировать по количеству неактивных клиентов по убыванию.')
df_j = (df_city
        .join(df_address, df_city['city_id'] == df_address['city_id'])
        .join(df_customer, df_address['address_id'] == df_customer['address_id'])
        )
df6 = (df_j
       .groupBy('city')
       .agg(
            sum(when(col('active') == 1, 1).otherwise(0)).cast('int').alias('active_customers'),
            sum(when(col('active') == 0, 1).otherwise(0)).cast('int').alias('inactive_customers')
            )
       .orderBy('inactive_customers', ascending=False)
       )
df6.show()

print('7.Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”.')
print('То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.')
df_j = (df_category
        .join(df_film_category, df_category['category_id'] == df_film_category['category_id'])
        .join(df_inventory, df_film_category['film_id'] == df_inventory['film_id'])
        .join(df_rental, df_inventory['inventory_id'] == df_rental['inventory_id'])
        .join(df_customer, df_rental['customer_id'] == df_customer['customer_id'])
        .join(df_address, df_customer['address_id'] == df_address['address_id'])
        .join(df_city, df_address['city_id'] == df_city['city_id'])
        )
df_f = df_j.filter((col('city').like('A%')) | (col('city').like('%-%')))
max_rental_hours = (df_f
                    .groupBy('name', 'city')
                    .agg(
                        sum(expr('COALESCE(EXTRACT(DAY FROM (return_date - rental_date)), 0)').cast('int'))
                        .alias('total_rental_hours')
                        )
                    )
max_hours_a = (max_rental_hours
               .filter(col('city').like('A%'))
               .selectExpr('MAX(total_rental_hours) as max_hours_a')
               .collect()[0]['max_hours_a']
               )
max_hours_b = (max_rental_hours
               .filter(col('city').like('%-%'))
               .selectExpr('MAX(total_rental_hours) as max_hours_b')
               .collect()[0]['max_hours_b']
               )
# union
df7 = (max_rental_hours
       .filter((col('city').like('A%') & (col('total_rental_hours') == max_hours_a)) |
               (col('city').like('%-%') & (col('total_rental_hours') == max_hours_b)))
       .select('name', 'city', 'total_rental_hours')
       )
df7.show()

# WRITE TO FILE
df7.write.option('header', 'true').mode('overwrite').csv('df7.csv')
df7.toPandas().to_csv("./2.csv", index=False)

# PLANS
plans = df7._jdf.queryExecution().toString()
plans1 = df7._sc._jvm.PythonSQLUtils.explainString(df7._jdf.queryExecution(), 'EXTENDED')
with open('./plans.txt', 'w') as file:
    file.write(plans)
with open('./plans1.txt', 'w') as file:
    file.write(plans1)
