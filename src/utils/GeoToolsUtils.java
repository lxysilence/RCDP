package utils;

import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.util.factory.GeoTools;
import org.locationtech.jts.geom.MultiLineString;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.FilterFactory2;


import java.io.*;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.Collections;

/**
 * 读取shp地图的开源库GeoTools和空间信息分析工具JTS
 */
public class GeoToolsUtils {

    public static void main(String[] args) {

    }

    public static double calculateLength(SimpleFeature simpleFeature, String geomAttrName) {
        MultiLineString multiLineString = (MultiLineString) simpleFeature.getAttribute(geomAttrName);
        return multiLineString.getLength();
    }

    /**
     * 更新shp文件某条记录的某个属性的值，因为是事务提交所以速度很慢，大批量修改不建议使用此方法
     * 注：修改一条记录时会使整个shp文件的中文乱码，暂时不用此方法
     * @param dataStore
     * @param fid
     * @param propertyName
     * @param value
     * @throws IOException
     */
    public static void updateShpValue(ShapefileDataStore dataStore, String fid, String propertyName, String value) throws IOException {
        Transaction transaction = new DefaultTransaction("change_"+propertyName+"_"+fid);
        String typeName = dataStore.getTypeNames()[0];
        SimpleFeatureStore store = (SimpleFeatureStore) dataStore.getFeatureSource(typeName);
        store.setTransaction(transaction);
        FilterFactory ff = CommonFactoryFinder.getFilterFactory( GeoTools.getDefaultHints() );
        Filter filter = ff.id(Collections.singleton(ff.featureId(fid)));
        try {
            store.modifyFeatures(propertyName, value, filter);
            transaction.commit();
        } catch(Exception eek){
            transaction.rollback();
            System.out.println("change_"+propertyName+"_"+fid+"：转换错误");
        }
    }

    /**
     * 根据属性名称和属性值查询features
     * @param featureSource
     * @param propertyName 属性名称
     * @param value 属性值
     * @return
     * @throws IOException
     */
    public static FeatureCollection<SimpleFeatureType, SimpleFeature> queryFeatures(SimpleFeatureSource featureSource, String propertyName, String value) throws IOException {
        FilterFactory2 ff2 = CommonFactoryFinder.getFilterFactory2();
        Filter filter = ff2.equals(ff2.property(propertyName), ff2.literal(value));
        return featureSource.getFeatures(filter);
    }

    public static void mlsToGeojson(MultiLineString multiLineString) {
        try {
            GeometryJSON geometryJson = new GeometryJSON(6);
            StringWriter writer = new StringWriter();
            geometryJson.write(multiLineString, writer);
            System.out.println(writer);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回一个可读取道路对象的迭代器
     * @param dataStore
     * @return
     */
    public static SimpleFeatureIterator readShp(ShapefileDataStore dataStore){
        try {
            if (dataStore == null) return null;
            SimpleFeatureSource featureSource = dataStore.getFeatureSource();
            if(featureSource == null)
                return null;
            return featureSource.getFeatures().features();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null ;
    }

    /**
     * 构建ShapeDataStore对象。
     * @param shpFilePath shape文件路径。
     * @return
     */
    public static ShapefileDataStore buildDataStore(String shpFilePath) {
        ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
        try {
            ShapefileDataStore dataStore = (ShapefileDataStore) factory
                    .createDataStore(new File(shpFilePath).toURI().toURL());
            if (dataStore != null) {
                dataStore.setCharset(Charset.forName("GBK"));
            }
            return dataStore;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 获取Shape文件的坐标系信息。但感觉好像识别不出GCJ-02
     *
     * @param shpFilePath shp文件路径
     * @return 坐标系的WKT形式
     */
    public static String getCoordinateSystemWKT(String shpFilePath) {

        ShapefileDataStore dataStore = buildDataStore(shpFilePath);

        try {
            return dataStore.getSchema().getCoordinateReferenceSystem().toWKT();
        } catch (UnsupportedOperationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            dataStore.dispose();
        }
        return "";
    }

}
