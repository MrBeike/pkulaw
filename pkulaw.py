import os, time, json, math
import requests
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from bs4 import BeautifulSoup


class PKULAW:
    def __init__(self):
        headers = {
            'Host': 'www.pkulaw.com',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'Sec-Fetch-User': '?1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        self.s = requests.session()
        self.s.headers = headers
        login_pre_url = 'https://login.pkulaw.com/'
        self.s.get(login_pre_url)
        # TODO 从配置文件里读取设置 可更改数据库类型
        engine = create_engine('sqlite:///datastore.db')
        self.con = engine.connect()

    def configReader(self):
        '''
        配置文件读取函数
        '''
        config = ConfigParser()
        config.read("config.ini", encoding="utf-8")
        self.username = config.get("userInfo", "username")
        self.password = config.get("userInfo", "password")

    def login(self):
        '''
        登陆函数
        username,password从配置文件内读取
        '''
        login_url = 'https://login.pkulaw.com/login?menu=&isAutoLogin=false&returnUrl=https://www.pkulaw.com/ '
        headers = {
            'Host': 'login.pkulaw.com',
            'Connection': 'keep-alive',
            'Content-Length': '28',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://login.pkulaw.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode':'cors',
            'Referer': 'https://login.pkulaw.com/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }
        data = {
            'LoginName': self.username,
            'LoginPwd' : self.password,
        }
        login_response = self.s.post(url=login_url, headers=headers,data=data).content.decode('utf-8')
        self.s.get(login_response).content.decode('utf-8')

    def search(self,targetWord='',pai='',category='',categoryCode='',year=2020):
        '''
        搜索函数
        :params targetWord->str 全文搜索关键词
        :params pai->str 案由的大类（统计搜索结果需要）
        :params category->str  案由的中文名称（统计搜索结果需要）
        :params categoryCode->str 案由的代码,可通过网页查找确认(code.xlsx)
        :params year->int  查询的年份（这里根据实际情况每次搜索一整年，其他需求可改AdvSearchDic.LastInstanceDate项目值）
        :return  response_collect->list  搜索结果页面合集（用于后面解析出gid）
        '''
        self.categoryCode = categoryCode
        self.year = year
        cookiejar = requests.utils.dict_from_cookiejar(self.s.cookies)
        sessionid = cookiejar['pkulaw_v6_sessionid']
        the_time = f'{time.time():10.0f}'  
        cookie = f'pkulaw_v6_sessionid={sessionid};Hm_lvt_8266968662c086f34b2a3e2ae9014bf8=1{the_time};xCloseNew=24;Hm_lpvt_8266968662c086f34b2a3e2ae9014bf8={the_time}'
        new_header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': cookie,
            'DNT': '1',
            'Host': 'www.pkulaw.com',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0',
        }
        self.s.headers = new_header
        # 必须先带cookie访问搜索地址，否则post会被拒绝
        search_pre_url = 'https://www.pkulaw.com/case/adv'
        self.s.get(search_pre_url).content.decode('utf-8')

        # 正式搜索POST
        request_headers = {
            'Host': 'www.pkulaw.com',
            'Connection': 'keep-alive',
            'Content-Length': '1272',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin':'https://www.pkulaw.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Referer': 'https://www.pkulaw.com/case/adv',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cookie':cookie,
        }
        search_url = 'https://www.pkulaw.com/case/search/RecordSearch'
        data = {
            'Menu':'case',    #司法案例
            'Keywords':'',
            'SearchKeywordType':'Fulltext',  #全文搜索
            'MatchType':'Exact',
            'RangeType':'Piece',  #全文-同篇
            'Library':'pfnl',  #库？ 司法案例？
            'ClassFlag':'pfnl',
            'GroupLibraries':'',
            'QueryOnClick':'False',
            'AfterSearch':'False',
            'PreviousLib':'pfnl',
            'pdfStr':'',
            'pdfTitle':'',
            'IsSynonymSearch':'true',
            'RequestFrom':'',
            'LastLibForChangeColumn':'pfnl',
            'IsAdv':'True',
            'ClassCodeKey':',,,,,,,,,', #未知  可不填？
            'GroupByIndex':'3', #分组模式  3为不分组(此项应固定)
            'OrderByIndex':'0', #排序模式  0为不排序
            'ShowType':'Default',
            'GroupValue':'',
            'AdvSearchDic.Title':'',
            'AdvSearchDic.CheckFullText':targetWord,
            'AdvSearchDic.Category':f'{categoryCode}#containAny', #案由  需要根据网站查询（包含条件） 如 00101,#containAny(contianAll/unContain)
            'AdvSearchDic.CaseFlag':'',
            'AdvSearchDic.LastInstanceCourt':'',
            'AdvSearchDic.CourtGrade':'',
            'AdvSearchDic.Judge':'',
            'AdvSearchDic.AgentLawyer':'',
            'AdvSearchDic.CaseClass':'002001', #案件类型 刑事一审（本例中固定）
            'AdvSearchDic.TrialStep':'',
            'AdvSearchDic.AgentLawOffice':'',
            'AdvSearchDic.TrialStepCount':'',
            'AdvSearchDic.DocumentAttr':'',
            'AdvSearchDic.LastInstanceDate':f"{{ 'Start': '{year}.01.01', 'End': '{year}.12.31' }}", #审结日期
            'AdvSearchDic.Core':'',
            'AdvSearchDic.CaseGist':'',
            'AdvSearchDic.DisputedIssues':'',
            'AdvSearchDic.CriminalPunish':'',
            'AdvSearchDic.CaseGrade':'',
            'AdvSearchDic.Criminal':'',
            'AdvSearchDic.Accusation':'',
            'AdvSearchDic.GuidingCaseNO':'',
            'AdvSearchDic.CivilLaw':'',
            'AdvSearchDic.IssueDate':"{ 'Start': '', 'End': '' }", #发布日期
            'AdvSearchDic.GuidingCaseDoc':'',
            'AdvSearchDic.SourceNote':'',
            'TitleKeywords':'',
            'FullTextKeywords':'',
            'Pager.PageIndex':'0',  #当前页
            'RecordShowType':'List',  #展示模式 （列表/图表）
            'Pager.PageSize':'100',  #每页显示条数（此项应固定）
            'QueryBase64Request':'',
            'VerifyCodeResult':'',
            'isEng':'chinese',
            'OldPageIndex':'', #页数跳转前所在页面数（遍历所有页面时，需提供此项）
            'newPageIndex':'',
            'X-Requested-With':'XMLHttpRequest',        
        }
        search_response = self.s.post(search_url,headers=request_headers,data=data).content.decode('utf-8')
        soup = BeautifulSoup(search_response,'html.parser')
        try:
            #获取检索结果条数
            count = soup.find(name='div',attrs={'class':'l-search min-height-30'}).h3.span.get_text()
            count = int(count)
        except:
            if soup.find(name='div',attrs={'class':'search-no-content'}):
                count = 0
            else:
                print('访问出错，请重试，若问题依旧存在，可能是IP被封')
                return False
        response_collect = []
        response_collect.append(search_response)
        #如果页数大于1,则循环收集剩下所有页面结果
        if count > 100:
            for i in range(1,math.ceil(count/100)+1):
                data['Pager.PageIndex'] = str(i)
                data['OldPageIndex'] =str(i-1)
                search_response = self.s.post(search_url,headers=request_headers,data=data).content.decode('utf-8')
                response_collect.append(search_response)
                time.sleep(1)
            # 第三项会重复 原因未知 hotfix
            del response_collect[2]
        with open('result.html','w',encoding='utf-8') as f:
            f.write(str(response_collect))
        return response_collect

    # 为保证下载成功 每次单页面解析  单页面下载 
    def parser(self,responseCollect):
        '''
        单页搜索结果解析
        :params responseCollect->list  搜索结果页面合集
        :return cases->dict  包含下载信息点的字典（用于后续下载信息表格）
        '''
        gid_list = []
        soup = BeautifulSoup(responseCollect,'html.parser')
        blocks = soup.findAll(name='div',attrs={'class':'block'})
        for block in blocks:
            gid = block.input.get('value')
            gid_list.append(gid)

        cases = []
        for i in range(len(gid_list)):
            single_case = {
                'idx':i+1,
                'gid':gid_list[i]
            }
            cases.append(single_case)
        return cases

    def download(self,cases,filename):
        '''
        下载搜索结果Excel表格
        :params cases->dict 下载的信息合集
        :params filename->str 下载的文件后缀（用于多文件命名）
        '''
        download_url = 'https://www.pkulaw.com/full/DownloadCatalog'
        download_json = json.dumps(cases)
        data = {
            'typeName':'catalogExcel',
            'condition':'',
            'library':'pfnl',
            'gids':download_json,
            'curLib':'pfnl',
            'fields':'序号,标题,审理程序,案由,文书类型,审理法院,案件字号,审结日期,省份,案件类型,原文链接',
        }
        response = self.s.post(download_url,data=data).content
        path = f'download/{self.category}'
        # TODO 判断文件夹是否存在
        if not os.path.exists(path):
            os.makedirs(path)
        fullName = f'{path}/{self.year}_{filename}.xls'
        with open(fullName,'wb') as f:
            f.write(response)

    def codeInitial(self):
        '''
        罪名Code对应表
        '''
        df = pd.read_excel('code.xlsx',converters = {u'code':str})
        df.fillna(method='pad',inplace=True)
        df.to_sql(name='code', con=self.con,index=False,if_exists='replace')

    # TODO 数据表设计
    def dataHandler(self,path):
        '''
        Excel表解析存入数据库函数
        :params path->str Excel文件所在的路径 
        '''
        # TODO 路径是否自动处理？os.path.isfile
        if os.path.exists(path):
            for file in os.listdir(path):
                df = pd.read_excel(f'{path}/{file}',usecols='B:K', header=[2], skipfooter=2)
                the_code = path.split('/')[1]
                code = [the_code] * len(df)
                df['code'] = code
                # df.insert(0,'code',code) 添加到第一列？
                # 读取链接,获取原文,存入数据库？
                links = df['原文链接'].to_list()
                articles = self.getArticle(links)
                df['原文'] = articles
                df.to_sql(name='cases', con=self.con,index=False,if_exists='append')

    def getArticle(self,links):
        '''
        获取原文内容函数
        :params links->list 原文Url链接
        '''
        articles = []
        for link in links:
            article_content = self.s.get(link).content.decode('utf-8')
            soup = BeautifulSoup(article_content,'html.parser')
            article = soup.find('div',attrs={'id':'divFullText','class':'fulltext'}).get_text()
            articles.append(article)
        return articles