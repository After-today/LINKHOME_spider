import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import random
import pymysql
import threadpool

# headers
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
        }

# 代理ip
conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="123456", db='my_sql', charset='utf8mb4')
cursor = conn.cursor()

sql = "SELECT * FROM proxies"
try:
    cursor.execute(sql)
    results = cursor.fetchall()
except Exception as e:
    print(e)
    conn.rollback()

proxies = []
for i in range(len(results)):
    val = 'https://' + results[i][0]
    p = {'http':val}
    proxies.append(p)
    
# 获取武汉市所有的大区域
url = 'https://wh.lianjia.com/chengjiao/'
res = requests.get(url, headers=headers)
soup_text = BeautifulSoup(res.text, 'html.parser')
areaList = soup_text.find('div', {'data-role': 'ershoufang'}).find_all('a')

# 获取所有的小区域
positionListItem = []
for area in areaList:
    ip = random.choice(proxies)
    url = 'https://wh.lianjia.com' + area['href']
    res = requests.get(url, headers = headers, proxies = ip, timeout=3)
    soup_text = BeautifulSoup(res.text, 'html.parser')
    positionList = soup_text.find('div', {'data-role': 'ershoufang'}).find_all('a')[15:]
    for position in positionList:
        if position not in positionListItem:
            positionListItem.append(area.text + ' ' + position['href'] + ' ' + position.text)
        else:
            continue
    print(len(positionListItem))

# 建立房产信息表
variables_list = ['houseHref', 'houseArea', 'housePosition', 'fixture_date', 'dealTotalPrice', 'dealAvgPrice',
                  'listedPrice', 'dealPeriod', 'adjustPrice', 'lookCount', 'followers', 'viewCount',
                  'houseShape', 'houseFloor', 'buildingArea', 'houseStructure', 'insideFloorArea', 'buildingTypes',
                  'houseOrientation', 'buildingTime', 'houseDecoration', 'buildingStructure', 'heatingMethod', 'LHRatio',
                  'liftEquip', 'tradingOwnership', 'listedTime', 'houseUsage', 'buildingAge', 'premisesOwnership',
                  'communityUnitPrice', 'propertyFee', 'totalBuildings', 'totalHouses', 'totalDeals']
houseInfo_list = 'C:/Users/Administrator/Desktop' + '/houseInfo.csv'
if not(os.path.exists(houseInfo_list)):
    houseInfo = pd.DataFrame(columns = variables_list)
    houseInfo.to_csv(houseInfo_list, index = None, encoding = "utf8")

# 房屋所在小区信息
def getCommunityInfo(soup_text):
    ip = random.choice(proxies)
    ID_community = soup_text.find('div', {'class': 'house-title'})['data-lj_action_housedel_id']
    url1_community = 'https://wh.lianjia.com/xiaoqu/' + ID_community
    res = requests.get(url1_community, headers = headers, proxies = ip, timeout = 30)
    soup_text = BeautifulSoup(res.text, 'html.parser')
    # 小区均价  元/m^2
    communityUnitPrice = soup_text.find('span', {'class': 'xiaoquUnitPrice'}).text
    # 小区物业费  元/平米/月
    propertyFee = soup_text.find_all('span', {'class': 'xiaoquInfoContent'})[2].text
    # 楼栋总数  栋
    totalBuildings = soup_text.find_all('span', {'class': 'xiaoquInfoContent'})[5].text[:-1]
    # 房屋总数  户
    totalHouses = soup_text.find_all('span', {'class': 'xiaoquInfoContent'})[6].text[:-1]

    url2_community = 'https://wh.lianjia.com/chengjiao/c' + ID_community
    res = requests.get(url2_community, headers = headers, proxies = ip, timeout = 30)
    soup_text = BeautifulSoup(res.text, 'html.parser')
    # 成交总数  套
    totalDeals = soup_text.find('div', {'class': 'total fl'}).find('span').text.strip()
    return communityUnitPrice, propertyFee, totalBuildings, totalHouses, totalDeals

# 获取房屋自身属性
def getHouseInfo(url, houseArea, housePosition, page):
    ip = random.choice(proxies)
    res = requests.get(url, headers = headers, proxies = ip, timeout = 30)
    soup_text = BeautifulSoup(res.text, 'html.parser')
    houseListContent = soup_text.find('ul', {'class': 'listContent'}).find_all('li')
    
    # 房屋基本信息
    for house in houseListContent:
        houseHref = house.find('a')['href']
        ip = random.choice(proxies)
        res = requests.get(houseHref, headers = headers, proxies = ip, timeout = 30)
        soup_text = BeautifulSoup(res.text, 'html.parser')
        
        # 成交日期
        fixture_date = soup_text.find('div', {'class': 'wrapper'}).find('span').text.split(' ')[0]
        info_fr = soup_text.find('div', {'class': 'info fr'})
        dealTotalPrice = info_fr.find('span').text      # 万     成交价格
        dealAvgPrice = info_fr.find('b').text           # 元/平  均价
        msg = info_fr.find('div', {'class': 'msg'}).find_all('span')
        listedPrice = msg[0].find('label').text         # 万     挂牌价格
        dealPeriod = msg[1].find('label').text          # 天     成交周期
        adjustPrice = msg[2].find('label').text         # 次     调价次数
        lookCount = msg[3].find('label').text           # 次     带看次数
        followers = msg[4].find('label').text           # 人     关注人数
        viewCount = msg[5].find('label').text           # 次     浏览次数
        
        introContent = soup_text.find('div', {'class': 'introContent'}).find_all('li')
        houseShape = introContent[0].text[4:].strip()           # 房屋户型
        houseFloor = introContent[1].text[4:].strip()           # 所在楼层
        buildingArea = introContent[2].text[4:].strip()         # 建筑面积
        houseStructure = introContent[3].text[4:].strip()       # 户型结构
        insideFloorArea = introContent[4].text[4:].strip()      # 套内面积
        buildingTypes = introContent[5].text[4:].strip()        # 建筑类型
        houseOrientation = introContent[6].text[4:].strip()     # 房屋朝向
        buildingTime = introContent[7].text[4:].strip()         # 建成年代
        houseDecoration = introContent[8].text[4:].strip()      # 装修情况
        buildingStructure = introContent[9].text[4:].strip()    # 建筑结构
        heatingMethod = introContent[10].text[4:].strip()       # 供暖方式
        LHRatio = introContent[11].text[4:].strip()             # 梯户比例
        #propertyRight = introContent[12].text[4:].strip()       # 产权年限
        liftEquip = introContent[12].text[4:].strip()           # 配备电梯
        tradingOwnership = introContent[14].text[4:].strip()    # 交易权属
        listedTime = introContent[15].text[4:].strip()          # 挂牌时间
        houseUsage = introContent[16].text[4:].strip()          # 房屋用途
        buildingAge = introContent[17].text[4:].strip()         # 房屋年限
        premisesOwnership = introContent[18].text[4:].strip()   # 房权所属
        
        try:
            communityInfo = getCommunityInfo(soup_text)
            communityUnitPrice = communityInfo[0]
            propertyFee = communityInfo[1]
            totalBuildings = communityInfo[2]
            totalHouses = communityInfo[3]
            totalDeals = communityInfo[4]
        except:
            error_list.append(houseHref)
            print('共计' + str(len(error_list)) + '条缺失')
            communityUnitPrice = ''
            propertyFee = ''
            totalBuildings = ''
            totalHouses = ''
            totalDeals = ''
        
        houseInfo = pd.DataFrame(
                {'houseHref': houseHref, 'houseArea': houseArea, 'housePosition': housePosition, 'fixture_date': fixture_date, 'dealTotalPrice': dealTotalPrice, 'dealAvgPrice': dealAvgPrice,
                 'listedPrice': listedPrice, 'dealPeriod': dealPeriod, 'adjustPrice': adjustPrice, 'lookCount': lookCount,'followers': followers, 'viewCount': viewCount,
                 'houseShape': houseShape, 'houseFloor': houseFloor, 'buildingArea': buildingArea, 'houseStructure': houseStructure, 'insideFloorArea': insideFloorArea, 'buildingTypes': buildingTypes,
                 'houseOrientation': houseOrientation, 'buildingTime': buildingTime, 'houseDecoration': houseDecoration, 'buildingStructure': buildingStructure, 'heatingMethod': heatingMethod, 'LHRatio': LHRatio,
                 'liftEquip': liftEquip, 'tradingOwnership': tradingOwnership, 'listedTime': listedTime, 'houseUsage': houseUsage, 'buildingAge': buildingAge, 'premisesOwnership': premisesOwnership,
                 'communityUnitPrice': communityUnitPrice, 'propertyFee': propertyFee, 'totalBuildings': totalBuildings, 'totalHouses': totalHouses, 'totalDeals': totalDeals}, 
                 columns = variables_list, index=[0])
        
        houseInfo.to_csv(houseInfo_list, index = None, mode = 'a', header = None, sep = ',', encoding = "utf-8")
        houseHrefList.append(houseHref)
        
        print('=====总第' + str(len(houseHrefList)) + '条=====')
    print('====第' + str(page) + '页 End, 总' + str(len(houseHrefList)) + '条====')

def get_houseInfo(position):
    # position = positionListItem[0]
    houseArea = position.split(' ')[0]
    housePosition = position.split(' ')[2]
    
    for page in range(1, 101):
        url = 'https://wh.lianjia.com' + position.split(' ')[1] + 'pg' + str(page)
        try:
            getHouseInfo(url, houseArea, housePosition, page)
        except:
            continue

error_list = []
houseHrefList = []
pool_size = 4
pool = threadpool.ThreadPool(pool_size)
#创建工作请求
reqs = threadpool.makeRequests(get_houseInfo, positionListItem)
#将工作请求放入队列
[pool.putRequest(req) for req in reqs]
pool.wait()
