package RCDPjour.generator;

import java.io.*;
import java.util.List;

public class Main_RCDP {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Code02_TrieTree_RCDP trie = new Code02_TrieTree_RCDP();

        //读取数据
        try (BufferedReader br = new BufferedReader(new FileReader("D:\\experiment\\RCDPjour\\commuteData\\commute_RCDP_adaptive\\25\\part-00000"))) {
            String line;
            while ((line = br.readLine()) != null) {
                trie.insert(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2. 深拷贝原始树状态（加噪前）
        Code02_TrieTree_RCDP originalTree = trie.copyTree();

        // 调用addNoiseAndRemoveNodes函数处理树中的节点
        trie.addNoiseAndRemoveNodes(1);

        trie.updateTree();

        // 4. 计算互信息
//        double mutualInfo = Code02_TrieTree_RCDP.calculateMutualInformation(originalTree.root, trie.root);
        double mutualInfo = Code02_TrieTree_RCDP.calculateNormalizedMutualInformation(originalTree.root, trie.root);
        System.out.println("Mutual Information: " + mutualInfo);

//        //输出生成数据
        List<String> words = trie.generateWordsFromTrie(trie);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\experiment\\RCDPjour\\genData\\RCDP\\200\\8.txt"))) {
            for (String sequence : words) {
                bw.write(sequence);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 记录程序结束时间
        long endTime = System.currentTimeMillis();

        // 计算并输出程序运行时间
        long elapsedTime = endTime - startTime;
        System.out.println("程序运行时间： " + elapsedTime + "毫秒");
    }

}
