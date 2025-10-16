package utils;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ExcelUtils {

    public static void main(String[] args) throws IOException {
        FileOutputStream file = new FileOutputStream("testData/test.xls");
        Workbook workbook = new HSSFWorkbook();
        Sheet sheet0 = workbook.createSheet("原始gps数据");
        ExcelUtils.buildExcelSheet(sheet0, Arrays.asList("原始数据","经度","纬度","时间","速度"), Arrays.asList(Arrays.asList("1","1","1","1"), Arrays.asList("1","1","1","1")));
        workbook.write(file);
    }

    public static void buildExcelSheet(Sheet sheet, List<String> titles, Collection<List<String>> rowList) {
        Row row0 = sheet.createRow(0);
        for (int i=0;i < titles.size();i++) {
            Cell cell = row0.createCell(i);
            cell.setCellValue(titles.get(i));
        }
        for (int i=0;i < rowList.size();i++) {
            Row row = sheet.createRow(i+1);
            List<String> rowData = rowList.iterator().next();
            for (int j = 0; j < rowData.size(); j++) {
                Cell cell = row.createCell(j);
                cell.setCellValue(rowData.get(j));
            }
        }
    }
}
