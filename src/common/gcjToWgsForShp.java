package common;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiLineString;
import org.opengis.feature.simple.SimpleFeature;
import utils.CommonUtils;
import utils.GPSUtils;
import utils.GeoToolsUtils;

/**
 * 将shp文件的坐标系由GCJ-02转换到WGS84
 * 实践过后发现这样太慢了，GeoTools的修改是通过事务的方式提交的，跑了一天多还没跑完，最后还是转用node.js完成的
 * 如果要用Java完成的话可以考虑先把数据导入到Mysql数据库里，批量读取修改再批量写入
 */
public class gcjToWgsForShp {
    public static void main(String[] args) {
        String filePath = CommonUtils.DATA_FACTORY_PATH + "RoadData\\sz_main_road_GCJ-02_revised\\mainRoad.shp";
        ShapefileDataStore dataStore = GeoToolsUtils.buildDataStore(filePath);
        if (dataStore == null) {
            System.out.println("读取地图文件失败");
            return;
        }
        SimpleFeatureIterator iters = GeoToolsUtils.readShp(dataStore);
        if (iters == null) {
            System.out.println("读取地图文件失败");
            return;
        }
        try {
            int i = 0,num = 0;
            while(iters.hasNext()) {
                SimpleFeature roadSF = iters.next();
                MultiLineString multiLineString = (MultiLineString) roadSF.getAttribute("the_geom");
                Coordinate[] coordinates = multiLineString.getCoordinates();
                if (coordinates.length == 0) continue;
                for(Coordinate coordinate : coordinates) {
                    double[] latAndLon = GPSUtils.gcj02_To_Gps84(coordinate.y, coordinate.x);
                    coordinate.setX(latAndLon[1]);
                    coordinate.setY(latAndLon[0]);
                }
                GeoToolsUtils.updateShpValue(dataStore, roadSF.getID(), "the_geom", multiLineString.toString());
                i++;
                num++;
                if (i == 1000) {
                    System.out.println("已完成数据转换"+num+"条！！！！！！！！！！！！！！！！！");
                    i = 0;
                }
            }
            System.out.println("<--------------------已完成所有数据转换，共计转换"+num+"条！!---------------------------->");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            iters.close();
            dataStore.dispose();
        }
    }

}
