import os
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

class DataStore:
    def __init__(self):
        # TODO 从配置文件里读取设置 可更改数据库类型
        engine = create_engine('sqlite:///datastore.db')
        self.con = engine.connect()

    def codeInitial(self):
        df = pd.read_excel('code.xlsx',converters = {u'code':str})
        df.fillna(method='pad',inplace=True)
        df.to_sql(name='code', con=self.con,index=False,if_exists='replace')
    
    # TODO 数据表设计
    def dataHandler(self,path):
        if os.path.exists(path):
            for file in os.listdir(path):
                df = pd.read_excel(f'{path}/{file}',usecols='B:K', header=[2], skipfooter=2)
                the_code = path.split('/')[1]
                print(len(df))
                code = [the_code] * len(df)
                print(code)
                df['code'] = code
                # df.insert(0,'code',code) 添加到第一列？
                # 读取链接,获取原文,存入数据库？
                links = df['原文链接'].to_list()
                articles = self.getArticle(links)
                df['原文'] = articles
                df.to_sql(name='cases', con=self.con,index=False,if_exists='append')

    def getArticle(self,links):
        articles = []
        for link in links:
            article_content = self.s.get(link).content.decode('utf-8')
            soup = BeautifulSoup(article_content,'html.parser')
            article = soup.find('div',attrs={'id':'divFullText','class':'fulltext'}).get_text()
            articles.append(article)
        return articles


d = DataStore()
d.codeInitial()
d.dataHandler('download/001030425')