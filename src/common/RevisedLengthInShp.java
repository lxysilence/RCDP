package common;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import utils.CommonUtils;
import utils.GeoToolsUtils;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 原shp文件有一些记录的Length值为0，重新计算修正一下
 * GeoTools提交修改会使shp文件里的中文全部乱码，暂时未解决。转用 在这里查询id然后拿到node.js中处理
 */
public class RevisedLengthInShp {

    //推断出的原数据与计算数据的大致换算比例
    public static final int CONVERSION_RATIO = 100000;

    public static void main(String[] args) throws IOException {
        String filePath = CommonUtils.DATA_FACTORY_PATH + "RoadData\\sz_main_road_GCJ-02_revised\\mainRoad.shp";
        ShapefileDataStore dataStore = GeoToolsUtils.buildDataStore(filePath);
        if (dataStore == null) {
            System.out.println("读取地图文件失败");
            return;
        }
        SimpleFeatureSource featureSource = dataStore.getFeatureSource();
        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = GeoToolsUtils.queryFeatures(featureSource, "Length", String.valueOf(0));
        FeatureIterator<SimpleFeature> featureIterator = featureCollection.features();
        int revisedNum = 0, deleteNum = 0;
        StringBuilder idsString = new StringBuilder();
        while (featureIterator.hasNext()) {
            SimpleFeature simpleFeature = featureIterator.next();
            //mainRoad.18850 => 18849
            String fid = (Integer.parseInt(simpleFeature.getID().split("\\.")[1]) - 1) + "";
            idsString.append(fid).append(",");
            double length = GeoToolsUtils.calculateLength(simpleFeature, "the_geom");
//            System.out.println(simpleFeature.getID() + ": " + length);
            double revisedLength = length * CONVERSION_RATIO;
            //与原数据保持一致保留六位小数
            DecimalFormat df = new DecimalFormat("#.000000");
            String revisedLengthSr = df.format(revisedLength);
            if (length == 0) {
                //考虑要不要删除
                //delete
                deleteNum++;
            } else {
                //暂时不用此方法
//                utils.GeoToolsUtils.updateShpValue(dataStore, simpleFeature.getID(), "Length", String.valueOf(revisedLength));
                System.out.println("修正 " + simpleFeature.getID() + ": " + revisedLengthSr);
                revisedNum++;
            }
        }
        System.out.println("共修正" + revisedNum + "条数据" + "，删除" + deleteNum + "条数据");
        System.out.println(idsString);
        CommonUtils.printlnToNewFile("output/length0_Fids.txt", idsString.toString());
    }




    //通过对比已有数据和jts getLength()计算出的数据推测出各自数据的长度单位
    //ID 962823 计算出的length为1.1173E-4, ID 962824 计算出的length为2.4569E-4, ID 962825 计算出的length为1.2306E-4, ID 962826 计算出的length为0.001253
    //所以虽然暂时不知道jts getLength()的单位，但是可得出原数据与计算数据的大致换算比例为10^5 : 1

//    public static void main(String[] args) throws IOException {
//        String filePath = "C:\\Users\\syc\\Desktop\\DataFactory\\RoadData\\sz_main_road_GCJ-02\\mainRoad.shp";
//        ShapefileDataStore dataStore = utils.GeoToolsUtils.buildDataStore(filePath);
//        if (dataStore == null) {
//            System.out.println("读取地图文件失败");
//            return;
//        }
//        SimpleFeatureSource featureSource = dataStore.getFeatureSource();
//        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = utils.GeoToolsUtils.queryFeatures(featureSource, "ID", String.valueOf(962835));
//        FeatureIterator<SimpleFeature> featureIterator = featureCollection.features();
//        while (featureIterator.hasNext()) {
//            SimpleFeature simpleFeature = featureIterator.next();
//            double length = utils.GeoToolsUtils.calculateLength(simpleFeature, "the_geom");
//            System.out.println(simpleFeature.getID() + ": " + length);
//        }
//    }
}
