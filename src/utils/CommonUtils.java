package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 通用工具方法。非必要时不用Scala,能用Java就用Java，毕竟Scala能兼容Java，但是Java里调用不了Scala的方法
 */
public class CommonUtils {

    public final static String DATA_FACTORY_PATH = "C:\\Users\\syc\\Desktop\\DataFactory\\";

    public static void main(String[] args) throws IOException {
        double lon1 = 114.047501;
        double lat1 = 22.645399;
        double lon2 = 114.047501;
        double lat2 = 22.6453;
        long begin = System.currentTimeMillis();
        double result = gpsDistance(lon1, lat1, lon2, lat2);
        long stop = System.currentTimeMillis();
        System.out.println(result);
        System.out.println(stop-begin);
        double errorValue = Math.pow(114.047501 - 114.047501, 2) + Math.pow(22.6453 - 22.645399, 2);
        System.out.println(errorValue);
    }


    public static InputStream getResourceFilePath(String fileName) throws FileNotFoundException {
        return CommonUtils.class.getClassLoader().getResourceAsStream(fileName);
    }

    /**
     * 根据路径 不存在则创建文件
     * @param filePath
     * @return
     */
    public static File createFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println("File exists");
        } else {
            System.out.println("File not exists, create it ...");
            //getParentFile() 获取上级目录（包含文件名时无法直接创建目录的）
            if (!file.getParentFile().exists()) {
                System.out.println("not exists");
                //创建上级目录
                file.getParentFile().mkdirs();
            }
            try {
                //在上级目录里创建文件
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }

    /**
     * 不存在则创建文件 存在则追加写
     */
    public static FileWriter getAppendWriter(String filePath) throws IOException {
        CommonUtils.createFile(filePath);
        return new FileWriter(filePath, true);
    }

    /**
     *  java 获取 获取某年某月 所有日期（yyyy-mm-dd格式字符串）
     * @param year
     * @param month
     * @return
     */
    public static List<String> getMonthFullDay(int year , int month){
        SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");
        List<String> fullDayList = new ArrayList<String>(32);
        // 获得当前日期对象
        Calendar cal = Calendar.getInstance();
        cal.clear();// 清除信息
        cal.set(Calendar.YEAR, year);
        // 1月从0开始
        cal.set(Calendar.MONTH, month-1 );
        // 当月1号
        cal.set(Calendar.DAY_OF_MONTH,1);
        int count = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        for (int j = 1; j <= count ; j++) {
            fullDayList.add(dateFormatYYYYMMDD.format(cal.getTime()));
            cal.add(Calendar.DAY_OF_MONTH,1);
        }
        return fullDayList;
    }

    /**
     * 获取[startDate, endDate]区间内的所有日期（yyyy-mm-dd格式字符串）
     */
    public static List<String> getDays(String startDate, String endDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        List<String> dateList = new ArrayList<String>();
        try {
            Date dateOne = sdf.parse(startDate);
            Date dateTwo = sdf.parse(endDate);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(dateOne);
            dateList.add(startDate);
            while (dateTwo.after(calendar.getTime())) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                dateList.add(sdf.format(calendar.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dateList;
    }

    public static void extractData() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("testData/GetMaxErrorRoad/maxErrorRoad.txt"), "utf-8"));

        FileWriter fw = new FileWriter("testData/roads.txt");
        BufferedWriter bw = new BufferedWriter(fw);

        String line = reader.readLine();
        while (line != null) {
            bw.write("'" + line.substring(1, line.length()-1).split(",")[0] + "',");
            line = reader.readLine();
        }
        bw.close();
    }

    /**
     * 将字符串内容写到一个新文件（不可覆盖）
     * @param filePath
     * @param content
     */
    public static void printlnToNewFile(String filePath, String content) {
        try {
            File file =new File(filePath);
            if(file.exists()){
                System.out.println("创建失败，" + filePath+ "文件已存在！！！！");
                return;
            }
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将UTC格式的时间字符串转换为对应的时间戳
     * todo 这里的异常应该抛出 Exception 但是已经有太多地方调用了不想改了 另写一个方法utcTimeToTS
     * @param utcTime
     * @return
     * @throws ParseException
     */
    @Deprecated
    public static long formatUtcTimeToTS(String utcTime) {
        String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            return simpleDateFormat.parse(utcTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static ThreadLocal<SimpleDateFormat> safeUtcSdf = new ThreadLocal<SimpleDateFormat> () {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));//设置时区为世界标准时UTC
            return simpleDateFormat;
        }
    };
    public static long utcTimeToTS(String utcTime) throws Exception {
//        String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
//        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return safeUtcSdf.get().parse(utcTime).getTime();
    }

    /**
     * 将指定pattern的时间字符串time转换为时间戳
     */
    public static long timeToTS(String time, String pattern) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return simpleDateFormat.parse(time).getTime();
    }

    /**
     * 将时间戳转换为时间
     */
    public static String stampToTime(long timestamp, String pattern) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        //将时间戳转换为时间
        Date date = new Date(timestamp);
        return simpleDateFormat.format(date);
    }

    /**
     * 将时间戳转换为时间
     */
    public static String stampToTime(long timestamp) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //将时间戳转换为时间
        Date date = new Date(timestamp);
        //将时间调整为yyyy-MM-dd_HH:mm:ss时间样式
        return simpleDateFormat.format(date);
    }

    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     *
     * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
     * el2 End altitude in meters
     * @returns Distance in Meters
     * from : https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
     */
    public static double gpsDistance(double lon1, double lat1, double lon2, double lat2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters


        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
    }

    /**
     * 将JSON字符串格式化输出
     */
    public static String jsonFormat(String jsonStr) {
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        return JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat);
    }
}
