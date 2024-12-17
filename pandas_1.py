import numpy as np
import pandas as pd
# Анализ и сравнение различных способов обработки и хранения больших данных:
# Pandas, Dask и Apache Spark
# Python – один из самых популярных языков программирования для работы с данными
# Для обработки больших данных на Python используются Pandas, NumPy, Dask, Apache Spark
# Pandas - это библиотека для обработки и анализа данных. Она предоставляет высокоуровневые
# структуры данных и мощные инструменты для манипуляций с ними. Pandas особенно хорош
# для работы с табличными данными.
# Dask – это библиотека для параллельных вычислений с данными, которая позволяет работать
# с массивами данных, превышающими оперативную память. Dask масштабируется от небольших
# кластеров до крупных дата-центров.
# Apache Spark – это платформа для кластерных вычислений, которая поддерживает множество
# языков, включая Python (через библиотеку PySpark). Spark отлично подходит для обработки
# больших данных в распределенных системах.

# преобразует словарь в таблицу, количество элементов списках должно быть одинаково
df = pd.DataFrame(
    {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
)
print(df)
# Pandas DataFrame активно применяют в машинном обучении, научных вычислениях и многих
# других областях, связанных с активным использованием данных. DataFrame быстрее, проще в
# использовании и мощнее SQL и Exsel, так как является неотъемлемой частью экосистемы Python.
# Выводит элементы первой строки
print('Элементы первой строки\n', df.loc[0], sep='')
# Выводит элементы первого столбца
print('Элементы столбца "ААА"\n', df.loc[:,"AAA"], sep='')
print('Cтроки таблицы в соответствии с условием: элемент столбца "ААА" <=5')
print(df[df.AAA <= 5])
# if-then метод loc присваивает соответствующим элементам столбца "BBB" 2 при выполнении
# условия
print('Замена элементов столбца "ВВВ" в соответствии с условием: элемент столбца "ААА" >=5')
df.loc[df.AAA >= 5, "BBB"] = 2
print(df)
print('Замена элементов столбцов "ВВВ" и "ССС" в соответствии с условием: элемент столбца "ААА" >=5')
df.loc[df.AAA >= 5, ["BBB", "CCC"]] = 10
print(df)
print(df.loc[(df["BBB"] > 25) | (df["CCC"] >= -40), "AAA"])
print(df)

#
df = pd.DataFrame(
    {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
)
#  Создаем маску
df_mask = pd.DataFrame(
    {"AAA": [True] * 4, "BBB": [False] * 4  ,"CCC":[True, False] * 2}
)
print('Булевая маска')
print(df_mask)
print('Замена элементов датафрейма на -10 взависимости от элементов маски:')
print(df.where(df_mask, -10))
# метод info() показывает информацию о наборе данных, индекс, столбцы, тип данных,
# ненулевые значения и использование памяти
print(df.info())



# метод Value_counts()
print(df['AAA'].value_counts([0]))
print(df['AAA'].value_counts())
# метод Describe пропускает строки и столбцы не содержащие чисел - категориальные признаки.
df.describe()

print(np.number)
# Добавление в датафрейм столбца
df['FFF']=[0,'NaN', 'NaN', 3]
df['EEE']=''
df.insert(1,'DDD','red')
print(df)
print(df.info())

# отбор числовых колонок
df_numeric = df.select_dtypes(include=[np.number])
print(df_numeric)
print(df_numeric.columns)
numeric_cols = df_numeric.columns.values
print(numeric_cols)

# отбор нечисловых колонок
df_non_numeric = df.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print(non_numeric_cols)
# изменение типа данных
df['EEE'] = pd.to_numeric(df['EEE'], errors='coerce')
df['FFF'] = pd.to_numeric(df['FFF'], errors='coerce').fillna(0)
print(df)
# df['DDD'].astype(dtype=str)
# df['EEE'].astype(dtype=int)

print(df.describe(include='all'))
print('*'*60)
# print(df)
# df['AAA'].describe()
# print(df['AAA'].describe())
# print(df.columns)
for col in df.columns:
    pct_missing = np.mean(df[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing*100)))

# подсчет определенного значения в столбце
print(df['AAA'].value_counts()[6])
print(df['AAA'].min())
print(df['AAA'].max())
print(df['AAA'].mean())
df['GGG']=[np.nan, np.nan,np.nan,np.nan]
df['KKK']=[10,5,9,11]
df['xxx']=['rr','yy','rr','yy']
print(df.groupby(['AAA']).max()[['KKK']])
print(df.groupby(['KKK']).min()[['AAA']])
print(df.groupby(['KKK']).max()[['AAA']])
print(df.groupby(['AAA']).max()[['BBB']])
print(df.groupby(['xxx']).groups)
print(df)
dfT=df.T
df=df.T.drop_duplicates().T
print(df)
df3=dfT.drop_duplicates()
print(dfT)
print(df3)
df=df3.T
print(df)

