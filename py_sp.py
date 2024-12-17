from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as f
from matplotlib import pyplot as plt
import pandas as pd


# Чтобы создать SparkSession, вам необходимо использовать метод builder().

# getOrCreate() возвращает уже существующий SparkSession; если он не существует,
# создается новый SparkSession. master(): если вы работаете с кластером, вам нужно
# передать имя своего кластерного менеджера в качестве аргумента. Обычно это будет либо
# yarn, либо mesos в зависимости от настройки вашего кластера, а при работе в автономном
# режиме используется local[x]. Здесь X должно быть целым числом, большим 0. Данное
# значение указывает, сколько разделов будет создано при использовании RDD, DataFrame
# и Dataset. В идеале X должно соответствовать количеству ядер ЦП. appName()
# используется для установки имени вашего приложения.

spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName('PySpark_Tutorial')\
        .getOrCreate()
print(spark.sparkContext.appName)
# Используя spark.read мы может считывать данные из файлов различных форматов, таких как
# CSV, JSON, Parquet и других. Вот несколько примеров получения данных из файлов
print('*'*60)
print('Чтение файла в датафрейм.')
data = spark.read.csv(
 'file_exz.csv',
 sep=',',
 header=True,
)

data.printSchema()
# Схема Spark отображает структуру фрейма данных или датасета. Мы можем определить ее с
# помощью класса StructType, который представляет собой коллекцию объектов StructField.
# Они в свою очередь устанавливают имя столбца (String), его тип (DataType), допускает ли
# он значение NULL (Boolean), и метаданные (MetaData). Spark автоматически выводит схему
# из данных, так как иногда предполагаемый им тип может быть неверным, или нам необходимо
# определить собственные имена столбцов и типы данных. Такое часто случается при работе с
# полностью или частично неструктурированными данными.
# Структурирование данных. В приведенном коде создается структура данных с помощью StructType
# и StructField. Затем она передается в качестве параметра schema методу spark.read.csv().
print('*'*60)
print('Структурирование датафрейма.')
from pyspark.sql.types import *

data_schema = [
               StructField('Index', IntegerType(), True),
               StructField('Customer Id', StringType(), True),
               StructField('First Name', StringType(), True),
               StructField('Last Name', StringType(), True),
               StructField('Company', StringType(), True),
               StructField('City', StringType(), True),
               StructField('Country', StringType(), True),
               StructField('Phone 1', StringType(), True),
               StructField('Phone 2', StringType(), True),
               StructField('Email', StringType(), True),
               StructField('Subscription Date', DateType(), True),
               StructField('Website', StringType(), True),
               StructField('age', IntegerType(), True),
               StructField('numberOfProducts', IntegerType(), True),
               StructField('amount_spent', IntegerType(), True),
               StructField('feedback', FloatType(), True),
               StructField('feedback1', FloatType(), True),
            ]

final_struc = StructType(fields = data_schema)

data= spark.read.csv(
    'file_exz.csv',
    sep=',',
    header=True,
    schema=final_struc
)


data.printSchema()

# Различные методы инспекции данных. Существуют следующие методы инспекции данных: schema,
# dtypes, show, head, first, take, describe, columns, count, distinct, printSchema.
# schema(): этот метод возвращает схему данных (фрейма данных).

# Различные методы инспекции данных. Существуют следующие методы инспекции данных: schema,
# dtypes, show, head, first, take, describe, columns, count, distinct, printSchema.
#
# schema(): этот метод возвращает схему данных (фрейма данных).
print('*'*60)
print('Схема данных (фрейма данных).')
print(data.schema)
# dtypes возвращает список кортежей с именами столбцов и типами данных
print('*'*60)
print('dtypes возвращает список кортежей с именами столбцов и типами данных.')
print(data.dtypes)
print('*'*60)
# head(n) возвращает n строк в виде списка
print('head(n) возвращает n строк в виде списка')
print(data.head(4))
# show() по умолчанию отображает первые 20 строк, а также принимает число
# в качестве параметра для выбора их количества.
print('*'*60)
print('Метод show() по умолчанию отображает первые 20 строк. Вывод show(7).')
data.show(7)
# first() возвращает первую строку данных
print('*'*60)
print('first() возвращает первую строку данных')
print(data.first())
# take(n) возвращает первые n строк
print('*'*60)
print('take(n) возвращает первые n строк. Вывод take(3)')
print(data.take(3))
# describe() вычисляет некоторые статистические значения для столбцов с числовым
# типом данных. Вычисляет базовую статистику для числовых и строковых столбцов.
print('*'*60)
print('Метод .describe() для столбцов с числовым типом данных data.describe(["age"])')
data.describe(['age']).show(5)
# columns возвращает список, содержащий названия столбцов
print('*'*60)
print('columns возвращает список, содержащий названия столбцов')
print(data.columns)
# count() возвращает общее число строк в датасете
print('*'*60)
print('count() возвращает общее число строк в датасете')
print(data.count())
# distinct() — количество различных строк в используемом наборе данных.
print('*'*60)
print('distinct() — количество различных строк в используемом наборе данных.')
print(data.distinct())
# printSchema() отображает схему данных.
print('*'*60)
print('printSchema() отображает схему данных.')
data.printSchema()
# Манипуляции со столбцами. Используются методы для добавления, обновления и удаления
# столбцов данных.
#
# Добавление столбца: withColumn, чтобы добавить новый столбец к существующим. Метод
# принимает два параметра: имя столбца и данные.
print('*'*60)
print('Добавление столбца "new_column"')
df = data.withColumn("new_column", data["age"]-18)
df.show(5)
# Переименованние столбца: withColumnRenamed. Метод принимает два параметра: название
# существующего столбца и его новое имя.
print('*'*60)
print('Переименованние столбца')
df = df.withColumnRenamed('new_column', 'age_changed')
df.show(5)
# Удаление столбца: метод drop, принимает имя столбца и возвращает данные.
print('*'*60)
print('Удаление столбца: метод drop')
df = df.drop('age_changed')
df.show(5)
# Работа с недостающими значениями:
# Удаление: удалить строки с пропущенными значениями в любом из столбцов.
# Замена средним/медианным значением: замените отсутствующие значения, используя среднее
# или медиану соответствующего столбца. Это просто, быстро и хорошо работает с небольшими
# наборами числовых данных.
# Замена на наиболее частые значения: как следует из названия, используйте наиболее часто
# встречающееся значение в столбце, чтобы заменить отсутствующие. Это хорошо работает с
# категориальными признаками, но также может вносить смещение (bias) в данные.
# Замена с использованием KNN: метод K-ближайших соседей — это алгоритм классификации,
# который рассчитывает сходство признаков новых точек данных с уже существующими, используя
# различные метрики расстояния, такие как Евклидова, Махаланобиса, Манхэттена, Минковского,
# Хэмминга и другие. Такой подход более точен по сравнению с вышеупомянутыми методами, но он
# требует больших вычислительных ресурсов и довольно чувствителен к выбросам.
# Удаление строк с пропущенными значениями data.na.drop()
# Замена отсутствующих значений средним
# data.na.fill(data.select(f.mean(data['feedback'])).collect()[0][0])
# Замена отсутствующих значений новыми data.na.replace(old_value, new_vallue)
print('*'*60)
print('.na.drop()')
df = data.na.drop()
df.show(5)
# Получение данных Метод select используется для выбора одного или нескольких столбцов
print('*'*60)
print('Метод select')
data.select('First Name', 'age').show(7)
# Метод filter фильтрует данные на основе заданного условия. Вы также можете указать
# несколько условий, используя операторы AND (&), OR (|) и NOT (~)
print('*'*60)
print('Метод filter 2020-01-01 - 2020-01-31')
data.filter( (col('Subscription Date') >= lit('2020-01-01')) & (col('Subscription Date') <= lit('2020-01-31')) ).show(5)
# Метод возвращает True, если проверяемое значение принадлежит указанному отрезку, иначе — False.
print('*'*60)
print('Метод filter age 30 - 33')
data.filter(data.age.between(30, 33)).show(10)
# Метод возвращает 0 или 1 в зависимости от заданного условия
print('*'*60)
print('.when()')


data.select('First Name', 'age',
 f.when(data.age >= 65, 1).otherwise(0)
).show(15)
# Метод rlike() для извлечения имен секторов, которые начинаются с указанных букв.
print('*'*60)
print('Метод rlike()')
data.select(
 'Country',
 data.Country.rlike('^[U,G]').alias('Колонка Country начинается с U или G')
).distinct().show()
# Функция groupBy группирует данные по выбранному столбцу и выполняет различные операции,
# такие как вычисление суммы, среднего, минимального, максимального значения и т. д.
print('*'*60)
print('Функция groupBy')
data.select(['Country', 'age', 'numberOfProducts'])\
    .groupBy('Country')\
    .mean()\
    .show()
# Агрегирование PySpark предоставляет встроенные стандартные функции агрегации, определенные
# в API DataFrame, они могут пригодится, когда нам нужно выполнить агрегирование значений
# ваших столбцов. Другими словами, такие функции работают с группами строк и вычисляют
# единственное возвращаемое значение для каждой группы.
print('*'*60)
print('Агрегирование')
data.filter((col('age') >= lit('30')) & (col('age') <= lit('60')))\
    .groupBy("Country") \
    .agg(f.min("age").alias("С"),
    f.max("age").alias("По"),

    f.min("numberOfProducts").alias("Минимальное количество подукции"),
    f.max("numberOfProducts").alias("Максимальное количество продукции"),
    f.avg("numberOfProducts").alias("Среднее в numberOfProducts"),

    f.min("amount_spent").alias("Минимальное количество средств"),
    f.max("amount_spent").alias("Максимальное количество средств"),
    f.avg("amount_spent").alias("Среднее в amount_spent"),

).show(truncate=False)
