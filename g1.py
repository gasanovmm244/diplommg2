import matplotlib.pyplot as plt

# функция формирует список из файла данных, который содержит затраченное время на
# формирование датафрейма из csv файла и поиск данных в датафрейме, а также суммирует их
def func(f):
    a = ''
    with open(f, 'r') as f:
        while True:
            a1 = f.readline()
            if not a1:
                break
            a = a + a1
    cc = ''
    yy = []
    for i in range(len(a)):
        if a[i].isdigit() or a[i] == '.':
            cc = cc + a[i]
        elif len(cc) != 0:
            yy.append(float(cc))
            cc = ''
    y_end = []
    for i in range(8):
        y_ = yy[i] + yy[i + 8]
        y_end.append(y_)
    return y_end

# Визуализация
y_p=func('1.txt')
y_d=func('2.txt')
# х - размер csv файла в Гб
x=[0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
# print(y_p)
# print(y_d)
plt.plot(x,y_p, color='blue')
plt.plot(x,y_d, color='green')
plt.xlabel('Размер csv файла в Гб')
plt.ylabel('Время в секундах')
plt.title('Время затраченное на обработку данных')
plt.annotate('Pandas', xy=(3, 1200), ha='right')
plt.annotate('Dask', xy=(3.5, 500), ha='right')

plt.show()


