package RCDPjour.generator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomData {
    public static void main(String[] args) {
        String fileName = "D:\\experiment\\data\\final\\random.txt"; // 数据文件名
        List<String> values = new ArrayList<>();
        List<Double> probabilities = new ArrayList<>();

        // 读取数据文件
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                values.add(parts[0]);
                probabilities.add(Double.parseDouble(parts[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 根据概率分布采样
        String sampledValue = sampleFromDistribution(values, probabilities);
        System.out.println("Sampled value: " + sampledValue);
    }

    private static String sampleFromDistribution(List<String> values, List<Double> probabilities) {
        double sum = 0;
        for (double probability : probabilities) {
            sum += probability;
        }
        double randomValue = new Random().nextDouble() * sum;
        double cumulativeProbability = 0;
        for (int i = 0; i < values.size(); i++) {
            cumulativeProbability += probabilities.get(i);
            if (randomValue <= cumulativeProbability) {
                return values.get(i);
            }
        }
        return "-1"; // 如果概率之和不为1，返回-1表示错误
    }
}