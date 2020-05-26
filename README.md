# LINKHOME_spider

本文采集了区位特征、房屋属性、交易指标三方面的数据，包括所属区域、建筑面积、所在楼层、挂牌价格等特征，均从链家网（http://wh.lianjia.com/）上取得；
小区信息从房屋所在小区详情页面获得；
周边配套设施的数据，如1km范围内的地铁站、公交站、幼儿园、电影院等设施的数量，则通过调用百度地图API服务获得。

在链家网上，武汉市区域被划分为15个区，共107个街道，每个页面展示30条房屋数据，通过翻页最多可以达到100页，即3000条数据。

为了能尽可能保证抓取到链家上所有的数据，根据深度优先算法思想，采用先遍历区域，再遍历街道的遍历思路来设计爬虫。