//gcood库测试
const gcoord = require('gcoord');

var result = gcoord.transform(
    [113.853891960474726, 22.60951992846616],    // 经纬度坐标
    gcoord.GCJ02,               // 当前坐标系
    gcoord.WGS84                // 目标坐标系
);

console.log(result);

var geojson = {
    type: 'LineString',
    coordinates: [
        [ 113.84395426417298, 22.61549727334661 ],
        [ 113.8440335605161, 22.61541817931749 ]
    ]
}
gcoord.transform(geojson, gcoord.GCJ02, gcoord.WGS84);
console.log(geojson.coordinates);