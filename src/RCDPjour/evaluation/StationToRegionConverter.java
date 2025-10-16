package RCDPjour.evaluation;
import java.io.*;
import java.util.*;

public class StationToRegionConverter {

    public static void main(String[] args) {
        // 文件路径配置
        String mappingFile = "D:\\experiment\\RCDPjour\\index\\Region\\part-00000";
        String inputFile = "D:\\experiment\\RCDPjour\\commuteData\\commute_Chen_15min\\part-00000";
        String outputFile = "D:\\experiment\\RCDPjour\\commuteData\\commute_Chen_region\\part-00000";

        // 1. 加载站点到区域ID的映射
        Map<String, String> siteToRegionMap = loadMappingData(mappingFile);

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

            String line;
            while ((line = reader.readLine()) != null) {
                // 2. 处理每行数据
                String processedLine = processLine(line, siteToRegionMap);
                if (processedLine != null) {
                    writer.write(processedLine);
                    writer.newLine();
                }
            }

            System.out.println("转换完成！结果已保存至: " + outputFile);

        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 加载站点区域映射数据
    private static Map<String, String> loadMappingData(String mappingFile) {
        Map<String, String> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(mappingFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length >= 2) {
                    String regionId = parts[0].trim();
                    String siteName = parts[1].trim();
                    // 避免重复站点覆盖
                    if (!siteName.isEmpty() && !map.containsKey(siteName)) {
                        map.put(siteName, regionId);
                    }
                }
            }
            System.out.println("已加载 " + map.size() + " 条站点映射记录");
        } catch (IOException e) {
            System.err.println("加载映射文件失败: " + e.getMessage());
            e.printStackTrace();
        }
        return map;
    }

    // 处理单行数据
    private static String processLine(String line, Map<String, String> siteToRegionMap) {
        String[] fields = line.split(",", -1);
        if (fields.length < 4) {
            System.err.println("忽略格式错误的行: " + line);
            return null;
        }

        // 处理出发字段
        String[] departInfo = splitFirstSpace(fields[0]);
        if (departInfo == null) {
            System.err.println("出发字段格式错误: " + fields[0]);
            return null;
        }
        String departTime = departInfo[0];
        String departRegion = siteToRegionMap.getOrDefault(departInfo[1], "UNKNOWN_SITE");

        // 处理到达字段
        String[] arriveInfo = splitFirstSpace(fields[1]);
        if (arriveInfo == null) {
            System.err.println("到达字段格式错误: " + fields[1]);
            return null;
        }
        String arriveTime = arriveInfo[0];
        String arriveRegion = siteToRegionMap.getOrDefault(arriveInfo[1], "UNKNOWN_SITE");

        // 拼接结果行
        return String.format("%s %s,%s %s,%s,%s",
                departTime, departRegion,
                arriveTime, arriveRegion,
                fields[2], fields[3]);
    }

    // 分割第一个空格：分成两部分
    private static String[] splitFirstSpace(String field) {
        int firstSpaceIndex = field.indexOf(' ');
        if (firstSpaceIndex <= 0 || firstSpaceIndex == field.length() - 1) {
            return null;
        }
        return new String[]{
                field.substring(0, firstSpaceIndex),
                field.substring(firstSpaceIndex + 1).trim()
        };
    }
}