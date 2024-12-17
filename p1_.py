import pandas as pd
import time
f_a=['a1.csv','a2.csv','a3.csv','a4.csv','a5.csv','a6.csv','a7.csv','a8.csv']


pd_rez=[]
pd_rez0=[]
with open('1.txt', 'w') as f:
    print(pd_rez0, file=f)
    print(pd_rez, file=f)
    f.close()

def time_work(df_):
    start = time.time()
    # df2 = df_['Country'].value_counts()['Liechtenstein']
    df3 = df_.loc[df_['First Name'] == 'Alex']
    end = time.time()
    rzlt = end - start
    print(f'Время работы: {rzlt} секунд')
    return rzlt

for i in f_a:
    start0=time.time()
    df4 = pd.read_csv(i).set_index('Index')
    end0=time.time()
    rz0=end0-start0
    pd_rez0.append(rz0)
    pd_rez.append(time_work(df4))
    print(pd_rez)

    with open('1.txt', 'w') as f:
        print(pd_rez0, file=f)
        print(pd_rez, file=f)
        f.close()

