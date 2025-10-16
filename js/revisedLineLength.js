//拿到geotools查询出的原shp文件中记录的Length值为0的id，重新计算修正数据写回shp文件
const gdal = require("gdal");
const fs = require('fs')

//r只读  r+读取修改增加  w创建, ESRI Shapefile即shp格式的全称
var dataset = gdal.open("C:\\Users\\syc\\Desktop\\DataFactory\\RoadData\\sz_main_road_GCJ-02_revised\\mainRoad.shp", "r+", "ESRI Shapefile");
var layer = dataset.layers.get(0);
var features = layer.features;

// for (i = 0;i < 10;i++) {
//     var feature = features.get(i);
//     console.log(feature.fid);
//     console.log(feature.fields.get('Length'))
//     var length = feature.getGeometry().getLength()
//     console.log(length)
//
// }

var num = 0;
var fidsString = fs.readFileSync('../output/length0_Fids.txt', 'utf8');
fidsString.replace(/\s*/g,"").split(",").filter(fid => fid != null && fid !== "" && !isNaN(fid)).forEach((fid) => {

    var feature = features.get(Number(fid))
    // console.log(feature.getGeometry().toJSON());
    var originalLength = feature.fields.get('Length')
    // console.log('fid' + feature.fid + '原Length值: ' + originalLength)
    if (originalLength === 0) {
        var revisedLength = feature.getGeometry().getLength() * 100000
        // console.log(revisedLength)
        feature.fields.set('Length', revisedLength)
        features.set(feature.fid, feature)
        console.log('fid' + feature.fid + ': 修正Length值' + originalLength + '为' + revisedLength)

        num++;
    }

});

//Flush pending changes to disk. 一定要调用这个方法，否则最后一条修改将不会生效
//目测features.set()方法里会调用这个方法，但是最后一条set就没人替它flush了，所以最后我们得手动调一下这个方法
layer.flush()
console.log('处理完成，共修正' + num + '条数据');