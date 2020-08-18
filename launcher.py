import os,shutil
import pandas as pd
from sqlalchemy import create_engine
from pkulaw import PKULAW

# 按年份分类文件到各文件夹
def move(path,targetPath,year):
    if not os.path.exists(targetPath):
        os.makedirs(targetPath)
    for file in os.listdir(path):
        the_year = file.split('_')[1]
        if int(the_year) == year:
            shutil.move(f'{path}/{file}',f'{targetPath}/{file}')

# 拼接同年份表格
def contact(path,year):
    df_sum = pd.DataFrame()
    for file in os.listdir(path):
        df = pd.read_excel(f'{path}/{file}',usecols='B:K', header=[2], skipfooter=2)
        the_code = file.split('_')[0]
        code = [the_code] * len(df)
        df['案由代码'] = code
        df_sum = df_sum.append(df)
    print(len(df_sum))
    df_sum_drop_duplicate = df_sum.drop_duplicates(subset='案件字号',keep="last")
    print(len(df_sum_drop_duplicate))
    df_sum_drop_duplicate.to_csv(f'{year}.csv')
    return df_sum_drop_duplicate

# 写入数据库
def write2Sql(df,pkulaw):
    for i in range(len(df)):
        df_new = df.iloc[[i]]
        link = df_new['原文链接'].to_list()[0]
        print(f'{i}:{link}')
        article = pkulaw.getArticle(link)
        df_new['原文'] = article
        df_new.to_sql(name='cases', con=pkulaw.con,index=False,if_exists='append')

# 断开后恢复 point 0-index
def resume(year,point,pkulaw):
    df = pd.read_csv(f'{year}.csv',usecols=['标题','审理程序','案由','文书类型','审理法院','案件字号','审结日期','省份','案件类型','原文链接','案由代码'])
    for i in range(point,len(df)):
        df_new = df.iloc[[i]]
        link = df_new['原文链接'].to_list()[0]
        print(f'{i}:{link}')
        article = pkulaw.getArticle(link)
        df_new['原文'] = article
        df_new.to_sql(name='cases', con=pkulaw.con,index=False,if_exists='append') 

if __name__ == '__main__':
    p = PKULAW()
    p.configReader()
    p.login()
    p.search(xxxxxx)
    # --------------
    path = '2018'
    year = 2018
    move('download',path,year)
    df_sum = contact(path,year)
    write2Sql(df_sum,p)
    # --------------
    resume(year,0,p)
