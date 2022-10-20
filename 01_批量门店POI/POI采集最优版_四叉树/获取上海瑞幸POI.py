# 该代码为获取上海瑞幸POI数据
# 第一步：获取上海边界范围外接矩形的坐标值
# 第二步：以四叉树索引的方式不断四分矩形，直到所有矩形符合POI数量限制
# 第三步：用来存放不符合POI数量限制多边形的列表
# 第四步：保存POI数据

import geopandas as gp
import pandas as pd
import requests

requests.packages.urllib3.disable_warnings()


# 判断输入多边形是否满足POI数量限制
# 假设POI数量限制为100
# 单条URL查询20条POI
# 则需要判断第6页的POI数量是否0
# 如果是0，则当前多边形满足POI数量限制，返回True
# 如果不为0，则当前多边形不满足POI数量限制，返回False
def judgeIfSatisfy_GaoDe(inputPolygon, key):
    # 组成Url
    currentUrl = "https://restapi.amap.com/v3/place/polygon?polygon=" + str(inputPolygon[0]) + "," + str(
        inputPolygon[2]) + "|" + str(inputPolygon[1]) + "," + str(
        inputPolygon[2]) + "|" + str(inputPolygon[1]) + "," + str(inputPolygon[3]) + "|" + str(
        inputPolygon[0]) + "," + str(inputPolygon[3]) + "|" + str(inputPolygon[0]) + "," + str(
        inputPolygon[2]) + "&offset=20&page=6&keywords=瑞幸&types=050000&output=json&key=" + key
    # 发送Get请求，并接收返回内容
    response = requests.get(currentUrl, stream=True, verify=False, timeout=60)
    returnData = response.json()
    # 获取数量POI数量
    resultCount = len(returnData['pois'])
    response.close()
    # 判断是否为0，并输出结果
    if resultCount > 0:
        return False
    else:
        return True


# 执行四叉树
# 对输入矩形进行四分
# 等分为左下角、右下角、右上角、左上角4个矩形
def executeQuadtree(minLon, maxLon, minLat, maxLat):
    resultList = []
    dLon = (maxLon - minLon) / 2
    dLat = (maxLat - minLat) / 2
    # 左下角的矩形
    resultList.append([minLon, minLon + dLon, minLat, minLat + dLat])
    # 右下角的矩形
    resultList.append([minLon + dLon, minLon + 2 * dLon, minLat, minLat + dLat])
    # 右上角的矩形
    resultList.append([minLon + dLon, minLon + 2 * dLon, minLat + dLat, minLat + 2 * dLat])
    # 左上角的矩形
    resultList.append([minLon, minLon + dLon, minLat + dLat, minLat + 2 * dLat])
    return resultList


# 从单个矩形获取瑞幸POI的方法
def getPoiFromPolygon(inputPolygon, key):
    # 存放POI数据结果的列表
    resultList = []
    # 由于POI数量限制是100，每页返回POI数量是20
    # 因此，最大的页数为5
    for currentPage in range(1, 6):
        # 组成Url
        currentUrl = "https://restapi.amap.com/v3/place/polygon?polygon=" + str(inputPolygon[0]) + "," + str(
            inputPolygon[2]) + "|" + str(inputPolygon[1]) + "," + str(
            inputPolygon[2]) + "|" + str(inputPolygon[1]) + "," + str(inputPolygon[3]) + "|" + str(
            inputPolygon[0]) + "," + str(inputPolygon[3]) + "|" + str(inputPolygon[0]) + "," + str(
            inputPolygon[2]) + "&offset=20&page=" + str(
            currentPage) + "&keywords=瑞幸&types=050000&output=json&key=" + key
        # 发送Get请求，并接收返回内容
        response = requests.get(currentUrl, stream=True, verify=False, timeout=60)
        returnData = response.json()
        returnPoiList = returnData['pois']
        if len(returnPoiList) > 0:
            for i in range(0, len(returnPoiList)):
                saveName = returnPoiList[i]['name']
                saveType = returnPoiList[i]['type']
                saveAddress = returnPoiList[i]['address']
                saveLocation = returnPoiList[i]['location']
                saveProvince = returnPoiList[i]['pname']
                saveCity = returnPoiList[i]['cityname']
                saveArea = returnPoiList[i]['adname']
                resultList.append(
                    [saveName, saveType, saveAddress, saveLocation, saveProvince, saveCity, saveArea])
                print(saveName, saveType, saveAddress, saveLocation, saveProvince, saveCity, saveArea)
        else:
            # 如果当前页POI数量为0，则返回已获取的POI数据并跳出
            return resultList
    return resultList


# 变量
gaodeKey = ""  # 高德地图API密钥，需要在高德官网申请，操作手册：https://kdocs.cn/l/coOAaCYwW0tg
pathShanghai = "D:\\上海边界数据.geojson"  # 上海边界
savePoiList = []  # 存放POI数据的结果列表
savePath = "D:\\poiresult.csv"  # POI数据存放地址

print("=======================================")
print("第一步：获取上海边界范围外接矩形的坐标值")
print("=======================================")
# 第一步：获取上海边界范围外接矩形的坐标值
# 通过读取GeoJSON格式的上海边界数据
# 获取上海边界外接矩形的最小经度、最大经度、最小纬度、最大纬度

# 上海边界经纬度值
resultMinLon = 99999.0  # 最小经度
resultMaxLon = 0.0  # 最大经度
resultMinLat = 99999.0  # 最小纬度
resultMaxLat = 0.0  # 最大纬度

# 读取上海边界数据
loadGeoData = gp.read_file(pathShanghai)
for i in range(0, len(loadGeoData)):
    # 读取几何数据
    loadGeometry = loadGeoData.loc[i, "geometry"]
    # 上海边界的几何数据是由多个多边形组成，如崇明岛、长兴岛、上海市区等
    # 所以需要遍历所有多边形
    # 多边形的类型是Polygon或者MultiPolygon
    for j in range(0, len(loadGeometry)):
        loadPolygon = loadGeometry[j]
        # 读取多边形的边界
        # 边界的类型是LineString(单线)或MultiLineString(多线)
        if loadPolygon.boundary.geom_type == 'LineString':
            # 读取边界中的每个坐标点
            for z in range(0, len(loadPolygon.boundary.coords)):
                loadLon = loadPolygon.boundary.coords[z][0]  # 经度
                loadLat = loadPolygon.boundary.coords[z][1]  # 纬度
                # 进行大小判断，并替换结果
                if loadLon <= resultMinLon:
                    resultMinLon = loadLon
                if loadLon >= resultMaxLon:
                    resultMaxLon = loadLon
                if loadLat <= resultMinLat:
                    resultMinLat = loadLat
                if loadLat >= resultMaxLat:
                    resultMaxLat = loadLat
        elif loadPolygon.boundary.geom_type == 'MultiLineString':
            # 如果边界是MultiLineString，则需要遍历其中的每条LineString
            for z in range(0, len(loadPolygon.boundary[0].coords)):
                loadLon = loadPolygon.boundary[0].coords[z][0]
                loadLat = loadPolygon.boundary[0].coords[z][1]
                if loadLon <= resultMinLon:
                    resultMinLon = loadLon
                if loadLon >= resultMaxLon:
                    resultMaxLon = loadLon
                if loadLat <= resultMinLat:
                    resultMinLat = loadLat
                if loadLat >= resultMaxLat:
                    resultMaxLat = loadLat
# 结果输出
print("上海最小经度：", resultMinLon)
print("上海最大经度：", resultMaxLon)
print("上海最小纬度：", resultMinLat)
print("上海最大纬度：", resultMaxLat)

print("=======================================")
print("第二步：以四叉树索引的方式不断四分矩形，直到所有矩形符合POI数量限制")
print("=======================================")
resultPolygonList = []  # 存放符合POI数量限制多边形的列表
currentPolygonList = [[resultMinLon, resultMaxLon, resultMinLat, resultMaxLat]]  # 用来存放不符合POI数量限制多边形的列表

# 以四叉树的方式不断四分外接矩形
for currentLevel in range(1, 9999999):
    # 用来存放不符合POI数量限制多边形的临时列表
    tempPolygonList = []
    for z in range(0, len(currentPolygonList)):
        loadMinLon = currentPolygonList[z][0]
        loadMaxLon = currentPolygonList[z][1]
        loadMinLat = currentPolygonList[z][2]
        loadMaxLat = currentPolygonList[z][3]
        # 判断当前矩形是否符合POI数量限制
        # 如果符合则放入resultPolygonList
        # 如果不符合则进行四分，并将四分结果放入tempPolygonList
        ifSatisfy = judgeIfSatisfy_GaoDe([loadMinLon, loadMaxLon, loadMinLat, loadMaxLat])
        if ifSatisfy == True:
            resultPolygonList.append([loadMinLon, loadMaxLon, loadMinLat, loadMaxLat])
        else:
            # 进行四叉树
            tempQuadtree = executeQuadtree(loadMinLon, loadMaxLon, loadMinLat, loadMaxLat)
            # 左下角
            tempPolygonList.append(tempQuadtree[0])
            # 右下角
            tempPolygonList.append(tempQuadtree[1])
            # 右上角
            tempPolygonList.append(tempQuadtree[2])
            # 左上角
            tempPolygonList.append(tempQuadtree[3])
    currentPolygonList = tempPolygonList.copy()
    # 如果没有多边形进入下一层级，则跳出
    print("第", currentLevel, "层", "......", "符合要求的矩形：", len(resultPolygonList), "......", "剩余矩形：",
          len(currentPolygonList))
    if len(currentPolygonList) == 0:
        break

print("=======================================")
print("第三步：遍历所有矩形，获取上海瑞幸POI")
print("=======================================")
for i in range(0, len(resultPolygonList)):
    # 读取每个矩形
    loadPolygon = resultPolygonList[i]
    # 获取矩形中的POI数据，并添加到结果列表中
    savePoiList.extend(getPoiFromPolygon(loadPolygon, gaodeKey))

print("=======================================")
print("第四步：保存获取的POI数据")
print("=======================================")
pd.DataFrame(savePoiList, columns=["name", "type", "address", "location", "pname", "cityname", "adname"]).to_csv(
    savePath, index=False)
print("完成POI数据获取", "......", "总共", len(savePoiList), "个POI数据")
