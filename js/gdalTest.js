//gdal库测试
//发现一个问题，使用node-gdal库写shp文件时会丢失.sbn和.sbx这两个空间索引文件

const gdal = require("gdal");

var dataset = gdal.open("C:\\Users\\syc\\Desktop\\DataFactory\\RoadData\\sz_main_road_GCJ-02_revised\\mainRoad.shp", "r+", "ESRI Shapefile");
//获取shp文件的第一个图层，我们的这个地图只有一个图层所以layers.get(0)即可
var layer = dataset.layers.get(0);
var count = layer.features.count();
console.log("number of features: " + layer.features.count());

for (i = 0;i < count;i=i+100) {
    var feature = layer.features.get(i);
    console.log(feature.fid);
    console.log(feature.getGeometry().toJSON());
}