import pandas as pd
import sys
df1 = pd.read_csv('file1.csv').set_index('Index')
print(df1.head())

a=(sys.getsizeof(df1))
print(a, 'b')

df_a=df1
while a>500:
    df_a=df_a.drop(df_a.index[-1])
    a=(sys.getsizeof(df_a))/1024
print('*'*20)
print(a, 'kb')
print(len(df_a))
df_a.to_csv('a.csv')
# df_a=df_a._append(df_a)
# a=sys.getsizeof(df_a)/1048576
# print(a, 'Mb')
# print(df_a.head())

# Оперативная память 4,00 ГБ (доступно: 3,83 ГБ)
# восемь датафреймов от 0.5 ГБ до 4.0 ГБ
# csv файл в 4 раза меньше датафрейма
# Создаем файлы от 0,5 до 4 Гб

a=0
while a<499*4:
    df_a=df_a._append(df_a)
    a=sys.getsizeof(df_a)/1048576
print('*'*30)
print(len(df_a))
print(a, 'Mb')
print(sys.getsizeof(df_a))
df_a.to_csv('a1.csv')
df_b=df_a

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a2.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a3.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a4.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a5.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a6.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a7.csv')

df_b=df_b._append(df_a)
a=sys.getsizeof(df_b)/1048576
print(sys.getsizeof(df_b))
print('*'*30)
print(len(df_b))
print(a, 'Mb')
df_b.to_csv('a8.csv')