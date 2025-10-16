//将shp文件的数据坐标系由GCJ-02改为WGS-84
const gcoord = require('gcoord');
const gdal = require("gdal");

//r只读  r+读取修改增加  w创建, ESRI Shapefile即shp格式的全称
var dataset = gdal.open("C:\\Users\\syc\\Desktop\\DataFactory\\RoadData\\sz_main_road_GCJ-02_revised\\mainRoad.shp", "r+", "ESRI Shapefile");
//获取shp文件的第一个图层，我们的这个地图只有一个图层所以layers.get(0)即可
var layer = dataset.layers.get(0);
//获取图层的的数据记录数，看看有没有正确读取到
console.log("number of features: " + layer.features.count());
var i = 0;
var sum = 0;
//遍历数据对每一条记录的坐标数据进行坐标系转换
layer.features.forEach((feature) => {

    //转换前
    // console.log(feature.fid);
    // console.log(feature.getGeometry().toJSON());
    // console.log(feature.fields.toJSON());

    //转换为JSON对象，用gcoord进行geom数据的坐标系转换
    var geojson = JSON.parse(feature.getGeometry().toJSON());
    //第二个参数为原坐标系，第三个参数为目标坐标系，gcoord支持多种坐标系的相互转换，具体可参考其文档
    gcoord.transform(geojson, gcoord.GCJ02, gcoord.WGS84);
    //转换后
    // console.log(geojson);

    //将转换后的数据保存回feature
    feature.setGeometry(gdal.Geometry.fromGeoJson(geojson));

    //将修改保存到shp文件中
    layer.features.set(feature.fid, feature);

    //对比发现保存后的数据点的经度的精度多出了一位，可能是保存浮点数过程中的一个小bug吧，不过没什么影响，尾数精度都十几位了
    // console.log(layer.features.get(feature.fid).getGeometry().toJSON());

    i++;
    sum++;
    if (i === 1000) {
        console.log("已完成数据转换" + sum + "条...............");
        i = 0;
    }
})

//Flush pending changes to disk. 一定要调用这个方法，否则最后一条修改将不会生效
//目测features.set()方法里会调用这个方法，但是最后一条set就没人替它flush了，所以最后我们得手动调一下这个方法
layer.flush()
console.log("<-----------------转换完成，共转换完成"+ sum + "条数据------------------------>");

//测试时也可用迭代器方法进行迭代，更多操作请参考node-gdal使用文档
// var feature = layer.features.next();

