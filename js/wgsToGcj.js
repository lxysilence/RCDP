const gcoord = require('gcoord');

var result = gcoord.transform(
    [114.06292, 22.5616],    // 经纬度坐标
    gcoord.WGS84,               // 当前坐标系
    gcoord.GCJ02               // 目标坐标系
);

console.log(result);