# -*- coding: utf-8 -*-
import findspark


from pyspark.sql import SparkSession

from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import to_date, when, count, window, desc, asc, sum



#historial de 1 mes de pagos realizados por los usuarios
pays_path = '/content/drive/MyDrive/dataSpark/CodeExercise DE - Carrusel XSelling 2/pays.csv'

#historial de 1 mes de value props que fueron mostradas a cada usuario
prints_path = '/content/drive/MyDrive/dataSpark/CodeExercise DE - Carrusel XSelling 2/prints.json'

#historial de 1 mes de value props que fueron clickeadas por un usuario
taps_path = '/content/drive/MyDrive/dataSpark/CodeExercise DE - Carrusel XSelling 2/taps.json'






findspark.init()

spark = SparkSession.builder \
    .appName("MercadoPago") \
    .getOrCreate()

"""# ***paysDF***

* historial de 1 mes de pagos realizados por los usuarios
"""

paysDF = spark.read.csv(path=pays_path, header=True)

paysDF = paysDF.withColumn('pay_date', to_date(paysDF['pay_date'], 'yyyy-MM-dd'))
paysDF = paysDF.withColumn('total', paysDF['total'].cast(IntegerType()))
paysDF = paysDF.withColumn('user_id', paysDF['user_id'].cast(IntegerType()))
paysDF = paysDF.withColumn('value_prop', paysDF['value_prop'].cast(StringType()))


"""# ***printsDF***

* historial de 1 mes de value props que fueron mostradas a cada usuario
"""

printsDF = spark.read.json(prints_path)


printsDF = printsDF.withColumn('day_prints', to_date(printsDF['day'], 'yyyy-MM-dd'))
printsDF = printsDF.withColumn('position_prints', printsDF['event_data.position'].cast(IntegerType())) \
                           .withColumn('value_prop_prints', printsDF['event_data.value_prop'].cast(StringType()))
printsDF = printsDF.drop('event_data')
printsDF = printsDF.drop('day')
printsDF = printsDF.withColumn('user_id', printsDF['user_id'].cast(IntegerType()))


"""# ***tapsDF***

* historial de 1 mes de value props que fueron clickeadas por un usuario
"""

tapsDF = spark.read.json(taps_path)


tapsDF = tapsDF.withColumn('day_taps', to_date(tapsDF['day'], 'yyyy-MM-dd'))
tapsDF = tapsDF.withColumn('position_taps', tapsDF['event_data.position'].cast(IntegerType())) \
                           .withColumn('value_prop_taps', tapsDF['event_data.value_prop'].cast(StringType()))
tapsDF = tapsDF.drop('event_data')
tapsDF = tapsDF.drop('day')
tapsDF = tapsDF.withColumn('user_id', tapsDF['user_id'].cast(IntegerType()))

"""***Desarrollo***

> Campo que indique si se hizo click o no
"""

prints_click_DF = printsDF.withColumn('click', when(printsDF['position_prints'] != 0, True).otherwise(False))


"""> Cantidad de veces que el usuario vio cada value prop en las 3 semanas previas a ese print."""

prints_view_quantity_DF = printsDF.groupBy('user_id', 'value_prop_prints', window('day_prints', '3 weeks').alias('three_weeks')).agg(count('*').alias('view_quantity'))


"""> Contar la cantidad de veces que el usuario clickeó cada value prop en las 3 semanas previas"""

taps_clicks_quantity_DF = tapsDF.groupBy('user_id', 'value_prop_taps', window('day_taps', '3 weeks').alias('three_weeks')).agg(count('*').alias('clicks_quantity'))


"""> Contar la cantidad de pagos que el usuario realizó para cada value prop en las 3 semanas previas"""

pays_count_DF = paysDF.groupBy('user_id', 'value_prop', window('pay_date', '3 weeks').alias('three_weeks')).agg(count('*').alias('pays_count'))


"""> Calcular importes acumulados que el usuario gastó para cada value prop en las 3 semanas previas"""

pays_total_DF = paysDF.groupBy('user_id', 'value_prop', window('pay_date', '3 weeks').alias('three_Weeks')).agg(sum('total').alias('pays_total'))
