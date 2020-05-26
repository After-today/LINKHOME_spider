import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import pymysql
import random

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
          }

# 建立周边设施信息表
facilities_list = ['地铁站', '公交站', '幼儿园', '小学', '中学', '大学', '医院', '药店', '商场', '超市',
                   '市场', '银行', 'ATM', '餐厅', '咖啡馆', '公园', '电影院', '健身房', '体育馆']
variables_list = ['houseHref', 'subwayStation', 'busStation', 'kindergarten', 'primarySchool',
                  'middleSchool', 'university', 'hospital', 'drugstore', 'mall', 'supermarket',
                  'markets', 'bank', 'ATM', 'restaurant', 'coffeeShop', 'park', 'cinema', 'GYM', 'stadium']

facilities_info = 'C:/Users/Administrator/Desktop' + '/facilitiesInfo.csv'
if not(os.path.exists(facilities_info)):
    facilities = pd.DataFrame(columns = variables_list)
    facilities.to_csv(facilities_info, index = None, encoding = "utf8")

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

# 获取房屋经纬度
def get_lat_long(a, b, c):
    par = a.partition(b)
    return par[2].partition(c)[0]

# 获取房屋设施信息
def get_info(url):
    ip = random.choice(proxies)
    res = requests.get(url, headers = headers, proxies = ip, timeout = 30)
    soup_text = BeautifulSoup(res.text, 'html.parser')
    
    lat_long = get_lat_long(str(soup_text.find_all('script')[-9]), 'resblockPosition:\'', '\'')
    lat_long = lat_long.split(',')[1] + ',' + lat_long.split(',')[0]
    
    info = []
    info.append(url)
    for fac in facilities_list:
        fac_url = 'http://api.map.baidu.com/place/v2/search?query=' + fac + '&location=' + lat_long + '&radius=1000&output=json&ak=dASz7ubuSpHidP1oQWKuAK3q'
        fac_soup_text = requests.get(fac_url, headers = headers, timeout = 30).text
        results = len(json.loads(fac_soup_text, strict = False)['results'])
        info.append(results)
    facilities = pd.DataFrame({'houseHref': info[0], 'subwayStation': info[1], 'busStation': info[2], 'kindergarten': info[3], 'primarySchool': info[4], 'middleSchool': info[5], 'university': info[6], 'hospital': info[7], 'drugstore': info[8],
                              'mall': info[9], 'supermarket': info[10], 'markets': info[11], 'bank': info[12], 'ATM': info[13], 'restaurant': info[14], 'coffeeShop': info[15], 'park': info[16], 'cinema': info[17], 'GYM': info[18], 'stadium': info[19]}, 
                               columns = variables_list, index=[0])
    facilities.to_csv(facilities_info, index = None, mode = 'a', header = None, sep = ',', encoding = "utf-8")

# 获取房屋url
houseInfo = pd.read_csv('C:/Users/Administrator/Desktop/houseInfo.csv')
Href_list = houseInfo['houseHref']

errorHref_list = []
for i in range(len(Href_list)):
    print(i)
    try: 
        get_info(Href_list[i])
    except:
        print((Href_list[i]) + '发生错误' + '总错误' + str(len(errorHref_list)))
        errorHref_list.append(Href_list[i])
