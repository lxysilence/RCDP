package RCDPjour.generator;

import java.util.*;

import static RCDPjour.generator.laplaceNoise.generateLaplaceRandom;

public class Code02_TrieTree_RCDP {

    public Node root;
    private int originalSize; // 新增字段，记录原始数据总数

    //构造函数，初始化根节点
    public Code02_TrieTree_RCDP() {
        this.root = new Node();
        this.originalSize = 0;
    }

    //定义内部类，表示树的节点
    public static class Node {
        public int pass;
        public int end;
        public HashMap<String, Node> nexts;

        public Node() {
            this.pass = 0;
            this.end = 0;
            this.nexts = new HashMap<>();
        }
        // 深拷贝构造函数
        public Node(Node other) {
            this.pass = other.pass;
            this.end = other.end;
            this.nexts = new HashMap<>();
            for (Map.Entry<String, Node> entry : other.nexts.entrySet()) {
                this.nexts.put(entry.getKey(), new Node(entry.getValue()));
            }
        }
    }

    //添加节点
    // 插入数据
    public void insert(String word) {
        if (word == null || "".equals(word)) return;
        String[] parts = word.split(",");
        Node node = root;
        node.pass++; // 当前节点的 pass 值（可能被噪声修改）
        originalSize++; // 原始数据总数 +1

        for (String part : parts) {
            if (!node.nexts.containsKey(part))
                node.nexts.put(part, new Node());
            node = node.nexts.get(part);
            node.pass++;
        }
        node.end++;
    }

    //序列总数
    public int size() {
        return originalSize;
    }

    // 深拷贝整棵树
    public Code02_TrieTree_RCDP copyTree() {
        Code02_TrieTree_RCDP copy = new Code02_TrieTree_RCDP();
        copy.root = new Node(this.root);
        copy.originalSize = this.originalSize;
        return copy;
    }

    // 遍历构建好的树，并为每个节点的pass值加入噪声，噪声值为该节点所在层值i
    // 如果加入噪声后的pass值小于-i+6则删除该节点

    public void addNoiseAndRemoveNodes(int i) {
        traverseAndAddNoise(root, i);
    }

    private void traverseAndAddNoise(Node node, int level) {
        if (node == null) return;

        // 为当前节点的pass值加入噪声
        double s = 1;     //灵敏度
        double epsilon = 0.8;
        double sigma = 1.1;
        double epsilon_l = Math.log(level+sigma)/(Math.log(1+sigma)+Math.log(2+sigma)+Math.log(3+sigma)+Math.log(4+sigma)+Math.log(5+sigma))*epsilon;
//        double epsilon_l = epsilon/5;
        double lambda = s / epsilon_l;// Scale parameter of the Laplace distribution

        // 从laplace分布中随机选点
        double noise = Math.round(generateLaplaceRandom(lambda));

        node.pass += noise;  //可改成关于level的函数

        // 如果加入噪声后的pass值小于阈值
        if (node.pass < 0) {
            node.pass = 0; // pass值置1
        }
//
        if (node.end > 0) { // 如果end值大于0
            node.end = node.pass; // 令end值等于pass值
        }else{
            node.end = 0;
        }

        // 遍历子节点，递归调用此方法
        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
            traverseAndAddNoise(entry.getValue(), level + 1);
        }
    }

    //生成结果序列
    // 生成结果序列（按pass值概率一条一条生成）
    public static List<String> generateWordsFromTrie(Code02_TrieTree_RCDP trie) {
        List<String> result = new ArrayList<>();
        int totalCount = trie.size(); // 获取输入数据总数
        Random random = new Random();

        // 逐条生成，直到达到总数
        while (result.size() < totalCount) {
            result.add(generateSingleWord(trie.root, random));
        }

        return result;
    }

    // 生成单个单词（按pass值概率选择路径）
    private static String generateSingleWord(Node root, Random random) {
        StringBuilder sb = new StringBuilder();
        Node current = root;

        while (true) {
            // 如果是叶子节点或者有结束标记，直接返回
            if (current.nexts.isEmpty() || current.end > 0) {
                return sb.toString();
            }

            // 计算所有子节点的总pass值
            int totalPass = current.nexts.values().stream().mapToInt(n -> n.pass).sum();
            if (totalPass <= 0) {
                return sb.toString();
            }

            // 根据pass值概率选择子节点
            int randomValue = random.nextInt(totalPass);
            int cumulative = 0;

            for (Map.Entry<String, Node> entry : current.nexts.entrySet()) {
                cumulative += entry.getValue().pass;
                if (randomValue < cumulative) {
                    // 添加路径分隔符（如果是非第一个元素）
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(entry.getKey());
                    current = entry.getValue();
                    break;
                }
            }
        }
    }


    //遍历所有节点存储内容
    public void traverse(Node node, String path) {

        if (node == null) return;
        // 打印节点信息
        System.out.println("Path: " + path + ", Pass: " + node.pass + ", End: " + node.end);
        for (Map.Entry<String, Node> entry : node.nexts.entrySet()) {
            traverse(entry.getValue(), path + "," + entry.getKey());
        }
    }

    public void traverse() {
        traverse(root, "");
    }

    // 层次遍历并输出节点内容
    public void levelOrderTraversal() {
        if (root == null) return;
        List<Node> queue = new ArrayList<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Node currentNode = queue.remove(0);
            System.out.println("节点： " + currentNode.pass + ", " + currentNode.end);
            for (Map.Entry<String, Node> entry : currentNode.nexts.entrySet()) {
                System.out.println("子节点： " + entry.getKey() + " -> " + entry.getValue().pass + ", " + entry.getValue().end);
                queue.add(entry.getValue());
            }
        }
    }

//    // 计算互信息
//    public static double calculateMutualInformation(Node root1, Node root2) {
//        // 收集两棵树的所有节点值
//        List<Integer> xValues = new ArrayList<>();
//        List<Integer> yValues = new ArrayList<>();
//        traverseTrees(root1, root2, xValues, yValues);
//
//        // 计算互信息
//        return calculateMI(xValues, yValues);
//    }
//
//    // 同时遍历两棵树并收集pass值
//    private static void traverseTrees(Node node1, Node node2, List<Integer> xValues, List<Integer> yValues) {
//        if (node1 == null || node2 == null) return;
//
//        // 添加当前节点的值
//        xValues.add(node1.pass);
//        yValues.add(node2.pass);
//
//        // 确保子节点顺序一致
//        List<String> sortedKeys = new ArrayList<>(node1.nexts.keySet());
//        Collections.sort(sortedKeys);
//
//        for (String key : sortedKeys) {
//            Node child1 = node1.nexts.get(key);
//            Node child2 = node2.nexts.get(key);
//            if (child1 != null && child2 != null) {
//                traverseTrees(child1, child2, xValues, yValues);
//            }
//        }
//    }
//
//    // 计算互信息
//    private static double calculateMI(List<Integer> xValues, List<Integer> yValues) {
//        int n = xValues.size();
//        if (n == 0) return 0.0;
//
//        // 计算联合分布和边缘分布
//        Map<String, Integer> jointCount = new HashMap<>();
//        Map<Integer, Integer> xCount = new HashMap<>();
//        Map<Integer, Integer> yCount = new HashMap<>();
//
//        for (int i = 0; i < n; i++) {
//            int x = xValues.get(i);
//            int y = yValues.get(i);
//
//            // 更新联合分布
//            String jointKey = x + "," + y;
//            jointCount.put(jointKey, jointCount.getOrDefault(jointKey, 0) + 1);
//
//            // 更新边缘分布
//            xCount.put(x, xCount.getOrDefault(x, 0) + 1);
//            yCount.put(y, yCount.getOrDefault(y, 0) + 1);
//        }
//
//        // 计算互信息
//        double mi = 0.0;
//        for (Map.Entry<String, Integer> entry : jointCount.entrySet()) {
//            String[] parts = entry.getKey().split(",");
//            int x = Integer.parseInt(parts[0]);
//            int y = Integer.parseInt(parts[1]);
//
//            double pXY = (double) entry.getValue() / n;
//            double pX = (double) xCount.get(x) / n;
//            double pY = (double) yCount.get(y) / n;
//
//            // 使用自然对数
//            mi += pXY * Math.log(pXY / (pX * pY));
//        }
//
//        return mi;
//    }

    // 计算标准互信息
    public static double calculateNormalizedMutualInformation(Node root1, Node root2) {
        // 收集两棵树的所有节点值
        List<Integer> xValues = new ArrayList<>();
        List<Integer> yValues = new ArrayList<>();
        traverseTrees(root1, root2, xValues, yValues);

        // 计算互信息和熵
        return calculateNMI(xValues, yValues);
    }

    // 同时遍历两棵树并收集pass值（保持不变）
    private static void traverseTrees(Node node1, Node node2, List<Integer> xValues, List<Integer> yValues) {
        if (node1 == null || node2 == null) return;

        // 添加当前节点的值
        xValues.add(node1.pass);
        yValues.add(node2.pass);

        // 确保子节点顺序一致
        List<String> sortedKeys = new ArrayList<>(node1.nexts.keySet());
        Collections.sort(sortedKeys);

        for (String key : sortedKeys) {
            Node child1 = node1.nexts.get(key);
            Node child2 = node2.nexts.get(key);
            if (child1 != null && child2 != null) {
                traverseTrees(child1, child2, xValues, yValues);
            }
        }
    }

    // 计算标准互信息（NMI）
    private static double calculateNMI(List<Integer> xValues, List<Integer> yValues) {
        int n = xValues.size();
        if (n == 0) return 0.0;

        // 计算联合分布和边缘分布
        Map<String, Integer> jointCount = new HashMap<>();
        Map<Integer, Integer> xCount = new HashMap<>();
        Map<Integer, Integer> yCount = new HashMap<>();

        for (int i = 0; i < n; i++) {
            int x = xValues.get(i);
            int y = yValues.get(i);

            // 更新联合分布
            String jointKey = x + "," + y;
            jointCount.put(jointKey, jointCount.getOrDefault(jointKey, 0) + 1);

            // 更新边缘分布
            xCount.put(x, xCount.getOrDefault(x, 0) + 1);
            yCount.put(y, yCount.getOrDefault(y, 0) + 1);
        }

        // 计算互信息和熵
        double mi = 0.0;  // 互信息
        double hX = 0.0;  // X的熵
        double hY = 0.0;  // Y的熵

        // 计算X的熵
        for (int count : xCount.values()) {
            double p = (double) count / n;
            hX -= p * Math.log(p);  // 使用自然对数
        }

        // 计算Y的熵
        for (int count : yCount.values()) {
            double p = (double) count / n;
            hY -= p * Math.log(p);  // 使用自然对数
        }

        // 计算互信息
        for (Map.Entry<String, Integer> entry : jointCount.entrySet()) {
            String[] parts = entry.getKey().split(",");
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            double pXY = (double) entry.getValue() / n;
            double pX = (double) xCount.get(x) / n;
            double pY = (double) yCount.get(y) / n;

            mi += pXY * Math.log(pXY / (pX * pY));
        }

        // 计算标准互信息（NMI）
        // 使用公式: NMI = 2 * MI / (H(X) + H(Y))
        if (hX + hY == 0) {
            return 0.0;  // 避免除零错误
        }
        return 2 * mi / (hX + hY);
    }


    public void updateTree() {
        if (root == null) return;
        updateNode(root);
    }


    //按权重
    private void updateNode(Node node) {
        if (node.nexts == null || node.nexts.isEmpty()) {
            return;
        }

        int childPassSum = 0;
        List<Node> children = new ArrayList<>(node.nexts.values());
        for (Node child : children) {
            updateNode(child);
            childPassSum += child.pass; // 累加前确保child.pass>=0
        }

        int diff = node.pass - childPassSum;

        if (diff == 0) {
            return;
        }

        if (diff > 0) {
            distributeExcess(children, diff, childPassSum);
        } else {
            distributeDeficit(children, -diff, childPassSum);
        }

        updateEndValues(children);

        if (diff != 0) {
            for (Node child : children) {
                updateNode(child);
            }
        }
    }

    private void distributeExcess(List<Node> children, int excess, int totalPass) {
        if (totalPass == 0) {
            int perNode = excess / children.size();
            int remainder = excess % children.size();
            for (int i = 0; i < children.size(); i++) {
                // 确保分配后的值>=0 (perNode可能为负？但excess>0所以不会)
                children.get(i).pass = Math.max(0, perNode + (i < remainder ? 1 : 0));
            }
            return;
        }

        int distributed = 0;
        Node maxNode = null;
        int maxPass = 0;

        for (Node child : children) {
            float ratio = (float) child.pass / totalPass;
            int adjustment = (int) (excess * ratio);

            if (child.pass > maxPass) {
                maxPass = child.pass;
                maxNode = child;
            }

            child.pass = Math.max(0, child.pass + adjustment); // 确保>=0
            distributed += adjustment;
        }

        int remaining = excess - distributed;
        if (remaining > 0 && maxNode != null) {
            maxNode.pass = Math.max(0, maxNode.pass + remaining); // 确保>=0
        }
    }

    // 完全重构的减少分配方法（确保pass>=0）
    private void distributeDeficit(List<Node> children, int reductionNeeded, int totalPass) {
        // 计算每个节点的理论减少量（浮点数比例）
        float[] ratios = new float[children.size()];
        for (int i = 0; i < children.size(); i++) {
            ratios[i] = (float) children.get(i).pass / totalPass;
        }

        // 第一轮：按比例分配整数减少量
        int[] reductions = new int[children.size()];
        int totalReduced = 0;

        for (int i = 0; i < children.size(); i++) {
            Node child = children.get(i);
            // 计算理论减少量并向下取整
            int reduction = (int) Math.floor(reductionNeeded * ratios[i]);
            // 确保减少量不超过当前pass值
            reduction = Math.min(reduction, child.pass);
            reductions[i] = reduction;
            totalReduced += reduction;
        }

        // 第二轮：处理剩余减少量
        int remainingReduction = reductionNeeded - totalReduced;
        if (remainingReduction > 0) {
            // 按节点剩余值降序排序（剩余值=当前pass - 计划减少量）
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < children.size(); i++) {
                indices.add(i);
            }
            indices.sort((i1, i2) -> {
                int remain1 = children.get(i1).pass - reductions[i1];
                int remain2 = children.get(i2).pass - reductions[i2];
                return Integer.compare(remain2, remain1); // 降序排序
            });

            // 从剩余值最大的节点开始分配剩余减少量
            for (int index : indices) {
                if (remainingReduction <= 0) break;

                // 计算当前节点还能减少的量
                int available = children.get(index).pass - reductions[index];
                if (available > 0) {
                    int deduct = Math.min(remainingReduction, available);
                    reductions[index] += deduct;
                    totalReduced += deduct;
                    remainingReduction -= deduct;
                }
            }
        }

        // 应用减少量并确保pass≥0
        for (int i = 0; i < children.size(); i++) {
            children.get(i).pass = Math.max(0, children.get(i).pass - reductions[i]);
        }
    }

    private void updateEndValues(List<Node> nodes) {
        for (Node node : nodes) {
            if (node.end > 0) {
                node.end = Math.max(0, node.pass); // 确保end值≥0
            }
        }
    }







////平均
//    private void updateNode(Node node) {
//        if (node.nexts == null || node.nexts.isEmpty()) {
//            return;
//        }
//
//        int childPassSum = 0;
//        List<Node> children = new ArrayList<>(node.nexts.values());
//        for (Node child : children) {
//            updateNode(child);
//            childPassSum += child.pass; // 累加前确保child.pass>=0
//        }
//
//        int diff = node.pass - childPassSum;
//
//        if (diff == 0) {
//            return;
//        }
//
//        if (diff > 0) {
//            distributeExcess(children, diff);
//        } else {
//            distributeDeficit(children, -diff);
//        }
//
//        updateEndValues(children);
//
//        if (diff != 0) {
//            for (Node child : children) {
//                updateNode(child);
//            }
//        }
//    }
//
//    // 修改后的平均分配方法
//    private void distributeExcess(List<Node> children, int excess) {
//        int count = children.size();
//        int perNode = excess / count;
//        int remainder = excess % count;
//
//        for (int i = 0; i < count; i++) {
//            int addAmount = perNode + (i < remainder ? 1 : 0);
//            children.get(i).pass += addAmount;
//        }
//    }
//
//    // 修改后的平均减少方法
//    private void distributeDeficit(List<Node> children, int reductionNeeded) {
//        int count = children.size();
//        int perNode = reductionNeeded / count;
//        int remainder = reductionNeeded % count;
//
//        for (int i = 0; i < count; i++) {
//            int deductAmount = perNode + (i < remainder ? 1 : 0);
//            Node child = children.get(i);
//
//            // 确保减少量不超过当前pass值
//            int actualDeduct = Math.min(deductAmount, child.pass);
//            child.pass -= actualDeduct;
//
//            // 调整剩余减少量
//            reductionNeeded -= actualDeduct;
//            deductAmount -= actualDeduct;
//
//            // 如果还有剩余减少量，尝试从其他节点扣除
//            if (deductAmount > 0) {
//                for (int j = i + 1; j < count && deductAmount > 0; j++) {
//                    Node otherChild = children.get(j);
//                    int extraDeduct = Math.min(deductAmount, otherChild.pass);
//                    otherChild.pass -= extraDeduct;
//                    deductAmount -= extraDeduct;
//                    reductionNeeded -= extraDeduct;
//                }
//            }
//        }
//
//        // 如果仍然有剩余减少量（理论上不应该发生），分配给第一个节点
//        if (reductionNeeded > 0) {
//            children.get(0).pass = Math.max(0, children.get(0).pass - reductionNeeded);
//        }
//    }
//
//    private void updateEndValues(List<Node> nodes) {
//        for (Node node : nodes) {
//            if (node.end > 0) {
//                node.end = Math.max(0, node.pass); // 确保end值≥0
//            }
//        }
//    }


//    //随机
//    private void updateNode(Node node) {
//        // 基本情况：叶子节点直接返回
//        if (node.nexts == null || node.nexts.isEmpty()) {
//            return;
//        }
//
//        // 第一次递归：更新所有子节点
//        int childPassSum = 0;
//        for (Node child : node.nexts.values()) {
//            updateNode(child);
//            childPassSum += child.pass;
//        }
//
//        Random rand = new Random();
//        int attempts = 0;
//        int maxAttempts = 1000; // 防止无限循环的安全阀
//
//        // 计算初始差值
//        int diff = node.pass - childPassSum;
//
//        // 只有当差值不为0时才进行调整
//        while (diff != 0 && attempts < maxAttempts) {
//            attempts++;
//
//            // 选择随机数量的子节点进行调整（至少1个，最多所有子节点）
//            int nodesToAdjust = rand.nextInt(node.nexts.size()) + 1;
//            List<Node> selectedNodes = new ArrayList<>(node.nexts.values());
//            Collections.shuffle(selectedNodes);
//            selectedNodes = selectedNodes.subList(0, Math.min(nodesToAdjust, selectedNodes.size()));
//
//            if (diff > 0) {
//                // 需要增加流量
//                for (Node child : selectedNodes) {
//                    // 随机增加量（至少1，最多剩余差值）
//                    int randomIncrease = rand.nextInt(diff) + 1;
//                    child.pass += randomIncrease;
//                    updateEndValue(child);
//                    diff -= randomIncrease;
//                    if (diff <= 0) break; // 提前结束
//                }
//            } else {
//                // 需要减少流量
//                for (Node child : selectedNodes) {
//                    // 跳过pass值已经为0的节点（不能减少）
//                    if (child.pass <= 0) continue;
//
//                    // 随机减少量（至少1，最多min(当前节点可减量, 需要减量)）
//                    int maxDecrease = Math.min(child.pass, -diff);
//                    int randomDecrease = rand.nextInt(maxDecrease) + 1;
//
//                    // 确保减少后不会小于0
//                    int newPass = child.pass - randomDecrease;
//                    child.pass = Math.max(0, newPass); // 确保最小值为0
//
//                    updateEndValue(child);
//                    diff += randomDecrease;
//                    if (diff >= 0) break; // 提前结束
//                }
//            }
//
//            // 重新计算子节点流量总和
//            childPassSum = 0;
//            for (Node child : node.nexts.values()) {
//                childPassSum += child.pass;
//            }
//            diff = node.pass - childPassSum; // 更新差值
//        }
//
//        if (attempts >= maxAttempts) {
//            // 强制平衡：平均分配剩余差值
//            int perNodeAdjust = diff / node.nexts.size();
//            int remainder = diff % node.nexts.size();
//            int index = 0;
//
//            for (Node child : node.nexts.values()) {
//                int adjustment = perNodeAdjust + (index < remainder ? 1 : 0);
//
//                // 确保调整后不会小于0
//                int newPass = child.pass + adjustment;
//                child.pass = Math.max(0, newPass); // 确保最小值为0
//
//                updateEndValue(child);
//                index++;
//            }
//        }
//
//        // 第二次递归：确保子节点内部平衡
//        for (Node child : node.nexts.values()) {
//            updateNode(child);
//        }
//    }
//
//    private void updateEndValue(Node node) {
//        if (node.end > 0) {
//            // 确保end值不会小于0
//            node.end = Math.max(0, node.pass);
//        }
//    }

}

