import random
import pandas as pd
import numpy as np

# Pandas работает с такими файлами как CSV, TXT, JSON, HTML и другими.

# Загрузка CSV файла из интернета
# Данные подписчиков
url1 = ('https://drive.google.com/uc?id=1OT84-j5J5z2tHoUvikJtoJFInWmlyYzY&export=download')
# количество ураганов
url = ('https://people.sc.fsu.edu/~jburkardt/data/csv/hurricanes.csv')

df = pd.read_csv(url)
df1 = pd.read_csv(url1).set_index('Index')
# df1 = pd.read_csv(url1, index_col=0)  # .set_index('id')

# Вывод первых 5 строк данных
print(df.head())
# количество строк и столбцов в файле
print(df.shape)
# Типы данных для каждого столбца
print(df.dtypes)
print(df1.dtypes)
print(df1.head())
# Вывод последних 2 строк
print(df1.tail(2))
# количество строк и столбцов в DataFrame df1
print(df1.shape)
# Типы данных для каждого столбца в DataFrame df1

# Сохранение в файл датафрейма
df1.to_csv('file1.csv')
# df1.to_csv('file1.csv', index=False, header=False)

print('*' * 60)
print('Метод .describe')
print(df1.describe(include='all'))
# describe() возвращает новый DataFrame с количеством строк, указанным в count, а также
# средним значением, стандартным отклонением, минимумом, максимумом и квартилями столбцов.
# Для категориальных признаков этот метод показывает: - Сколько уникальных значений в
# наборе данных - unique; top значения; частота появления значений - freg.
print('*' * 60)
print(df1.info())
print('*' * 60)
# Выводит элементы первой строки
num = 555
print(f'Элементы {num} строки\n', df1.loc[num], sep='')
# Выводит элементы первого столбца
print('Элементы столбца "Company"\n', df1.loc[:, "Company"], sep='')
print('*' * 60)
print('Метод .describe', ' Company')
print(df1['Company'].describe(include='all'))
print('*' * 60)
print(df1['Company'].value_counts()['Benitez LLC'])
print(df1.loc[df1['Company'] == 'Benitez LLC'])
print('*' * 60)
print('Метод .describe', ' Country')
print(df1['Country'].describe(include='all'))
print(df1.loc[df1['Country'] == 'Liechtenstein'])
print('*' * 60)
print(df1['Country'].value_counts()['Liechtenstein'])
print('*' * 60)
print('Метод .describe', ' First Name')
print(df1['First Name'].describe(include='all'))
print(df1.loc[df1['First Name'] == 'Larry'])
print('*' * 60)
# Поиск дубликатов
duplicateRows = df1[df1.duplicated()]
print('Одинаковые строки')
print(duplicateRows)

dfT = df1.T
duplicate_dft = dfT[dfT.duplicated()]
print('Одинаковые столбцы')
print(duplicate_dft)
print('*' * 60)
df1['Subscription Date'] = pd.to_datetime(df1['Subscription Date'])
# по возрастанию
df_data = df1.sort_values(by='Subscription Date')
print(df_data.head())
print(df1.head())
# df.sort_values (by='date', ascending= False) # по убыванию

# добавим столбцы
age = []
numberOfProducts = []
amount_spent = []
feedback = []
feedback1 = []
n = np.nan
for i in range(1000):
    age.append(random.randint(18, 70))
    number = random.randint(1, 1000)
    price = random.randint(50, 500)
    a = random.randint(1, 10)
    b = random.randint(1, 10)
    feedback.append(random.choice(['NaN', a, b]))
    numberOfProducts.append(number)
    amount_spent.append(number * price)
df1['age'] = age
df1['numberOfProducts'] = numberOfProducts
df1['amount_spent'] = amount_spent
df1['feedback'] = feedback
df1['feedback1'] = feedback
df1['feedback'] = pd.to_numeric(df1['feedback'], errors='coerce')
df1['feedback1'] = pd.to_numeric(df1['feedback1'], errors='coerce')
print(df1.dtypes)
# Сохранение датафрейма в файл для испытания в других файлах
df1.to_csv('file_exz.csv')
# отбор числовых колонок
df_numeric = df1.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values
print(numeric_cols)

# отбор нечисловых колонок
df_non_numeric = df1.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print(non_numeric_cols)

for col in df1.columns:
    pct_missing = np.mean(df1[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing * 100)))


# удаление дубликатов
# df1=df1.T.drop_duplicates().T # один из вариантов
def getDuplicateColumns(df):
    dupColName = set()
    for i in range(df.shape[1]):
        col = df.iloc[:, i]
        for k in range(i + 1, df.shape[1]):
            otherCol = df.iloc[:, k]
            if col.equals(otherCol):
                dupColName.add(df.columns.values[k])
    return list(dupColName)


rslt_df = df1.drop(columns=getDuplicateColumns(df1))
print(rslt_df.dtypes)

for col in rslt_df.columns:
    pct_missing = np.mean(rslt_df[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing * 100)))

print('*' * 60)
print('Метод .describe', ' Country')
print(rslt_df['Country'].describe(include='all'))
print(rslt_df.loc[rslt_df['Country'] == 'Liechtenstein'])
print('*' * 60)
# вывод максимальногоб минимального и среднего значение возраста
print(rslt_df['age'].min())
print(rslt_df['age'].max())
print(rslt_df['age'].mean())
print(rslt_df['amount_spent'].sum())
# Создание нового столбца с определением группы по возрасту
rslt_df['age_level'] = rslt_df['age'].apply(lambda x: 'yang' if x < 30 else 'midlle' if x < 60 else 'old')
print(rslt_df.dtypes)
print(rslt_df.head(7))
print('*' * 60)
# Замена недостающих значений медианой
med = rslt_df['feedback'].median()
print(med)
rslt_df['feedback'] = rslt_df['feedback'].fillna(med)
print(rslt_df.head(7))
rslt_df.to_csv('file2.csv')
