import pandas as pd
from bs4 import BeautifulSoup

htmlfile = open('code.html', 'r', encoding='utf-8')
htmlhandle = htmlfile.read()
soup = BeautifulSoup(htmlhandle,'html.parser')
blocks = soup.findAll(name='option')
code_collect = []
title_collect= []
for block in blocks:
    code = block.get('lvalue')
    title = block.get('title')
    code_collect.append(code)
    title_collect.append(title)
code_dict = {
    'title':title_collect,
    'code':code_collect
}

df = pd.DataFrame.from_dict(code_dict)
df['code']=df['code'].astype(str)
df.to_excel('code.xlsx',index=False)