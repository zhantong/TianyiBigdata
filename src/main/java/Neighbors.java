import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * 找出与指定用户最相似的其他用户
 */
public class Neighbors {
    private static List<String> toFind = new ArrayList<>();//需要寻找相似用户的用户

    /**
     * 将 关键字编号1:访问次数1;关键字编号2:访问次数2;.... 转换为Map
     *
     * @param str 关键字编号1:访问次数1;关键字编号2:访问次数2;....
     * @return {关键字编号1:访问次数1;关键字编号2:访问次数2;....}
     */
    private static HashMap<Integer, Integer> toMap(String str) {
        HashMap<Integer, Integer> map = new HashMap<>();
        String[] items = str.split(";");
        for (String item : items) {
            String[] split = item.split(":");
            map.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
        }
        return map;
    }

    /**
     * 向量长度
     *
     * @param collection 向量
     * @return 长度, 即内积开方
     */
    private static double deviation(Collection<Integer> collection) {
        double sum = 0f;
        for (Integer a : collection) {
            sum += a * a;
        }
        return Math.sqrt(sum);
    }

    /**
     * 计算指定向量与其他向量相似度
     *
     * @param key      需要寻找相似用户的用户名编号
     * @param userInfo 用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;.... 映射关系
     * @param set      所有可能与其相似的其他用户
     * @return 以相似程度排序后的其他用户名编号, 和相似度
     */
    private static java.util.Map calcDistance(IntWritable key, Hashtable<String, String> userInfo, Set<IntWritable> set) {
        HashMap<String, Double> map = new HashMap<>();
        String left = key.toString();
        HashMap<Integer, Integer> mapA = toMap(userInfo.get(left));//需要寻找相似用户的用户访问信息
        for (IntWritable item : set) {//遍历与其可能相关的其他用户
            String right = item.toString();
            HashMap<Integer, Integer> mapB = toMap(userInfo.get(right));
            map.put(right, calc(mapA, mapB));//计算相似度并加入新的Map
        }
        return utils.sortByValue(map);//以相似度由高到低排序
    }

    /**
     * 计算相似度, 余弦距离相似性算法
     * 相似度在-1~1之间, 1代表完全相同, 0代表相互独立, -1代表完全相反
     *
     * @param mapA Map A
     * @param mapB Map B
     * @return 相似度
     */
    private static double calc(HashMap<Integer, Integer> mapA, HashMap<Integer, Integer> mapB) {
        double sum = 0f;
        for (int keyA : mapA.keySet()) {
            if (mapB.containsKey(keyA)) {
                sum += mapA.get(keyA) * mapB.get(keyA);
            }
        }
        //可能溢出
        return sum / (deviation(mapA.values()) * deviation(mapB.values()));
    }

    /**
     * 指定项是否存在于数组中
     *
     * @param array 数组
     * @param item  指定项
     * @return 存在返回true, 否则false
     */
    private static boolean contain(String[] array, String item) {
        for (String val : array) {
            if (val.equals(item)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Mapper
     * 找出所有与需要寻找相似用户的用户存在相似的其他用户
     * 两个用户在同一个网站关键字下即存在相似
     * 输入: key: LongWritable 当前行偏移距离                    value: Text 当前行内容, 关键字编号+用户名编号1,用户名编号2,用户名编号3...
     * 输出: key: IntWritable 用户名编号(需要寻找相似用户的用户)    value: IntWritable 用户名编号(与其相似的用户)
     */
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        /**
         * map setup
         * 得到需要寻找相似用户的用户名编号集合
         *
         * @param context 上下文
         */
        public void setup(Mapper.Context context) {
            URI[] cacheFiles;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String line;
                cacheFiles = context.getCacheFiles();
                Path pathToFind = new Path(cacheFiles[0]);//DistributedCache
                FSDataInputStream fsinToFind = fs.open(pathToFind);
                DataInputStream inToFind = new DataInputStream(fsinToFind);
                BufferedReader readerToFind = new BufferedReader(new InputStreamReader(inToFind));
                while ((line = readerToFind.readLine()) != null) {
                    toFind.add(line);//加入List
                }
                readerToFind.close();
                inToFind.close();
                fsinToFind.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * map
         * 寻找与需要寻找相似用户的用户相似的用户, 采用协同过滤的思想, 即只有与此用户有相同访问网站关键字的用户, 才可能与其相似
         * 而不需要从所有的用户中寻找
         *
         * @param key     当前行偏移距离
         * @param value   当前行内容, 关键字编号+用户名编号1,用户名编号2,用户名编号3...
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String[] users = split[1].split(",");
            for (String item : toFind) {//遍历需要寻找相似用户的用户
                if (contain(users, item)) {//如果存在于这个关键字中,则此关键字下所有用户都可能与其相似
                    IntWritable first = new IntWritable(Integer.parseInt(item));
                    for (String second : users) {
                        if (!second.equals(item)) {
                            context.write(first, new IntWritable(Integer.parseInt(second)));
                        }
                    }
                }
            }
        }
    }

    /**
     * Reducer
     * 寻找出所有与指定用户可能相似的用户, 并计算相似度, 寻找出相似度最高的若干用户
     * 输入: key: IntWritable 用户名编号(需要寻找相似用户的用户)    value: Iterable<IntWritable> [用户名编号1,用户名编号2,...] (与其相似的用户)
     * 输出: key: IntWritable 不变                              value: 用户名编号1:相似度1;用户名编号2:相似度2;... (以相似度由大到小排序)
     */
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        private Hashtable<String, String> userInfo = new Hashtable<>();//用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;....
        private final static int neighborsNum = 20;

        /**
         * reduce setup
         * 读取 用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;.... 映射
         *
         * @param context 上下文
         */
        public void setup(Reducer.Context context) {
            URI[] cacheFiles;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String line;
                cacheFiles = context.getCacheFiles();//DistributedCache
                Path pathToFind = new Path(cacheFiles[1]);
                FSDataInputStream fsinToFind = fs.open(pathToFind);
                DataInputStream inToFind = new DataInputStream(fsinToFind);
                BufferedReader readerToFind = new BufferedReader(new InputStreamReader(inToFind));
                while ((line = readerToFind.readLine()) != null) {
                    String[] tokens = line.split("\t", 2);
                    userInfo.put(tokens[0], tokens[1]);
                }
                readerToFind.close();
                inToFind.close();
                fsinToFind.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * reduce
         *
         * @param key     用户名编号(需要寻找相似用户的用户)
         * @param values  [用户名编号1,用户名编号2,...] (与其相似的用户)
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Set<IntWritable> set = new HashSet<>();//与其相似的用户的集合
            StringBuilder builder = new StringBuilder();
            for (IntWritable val : values) {
                if (!set.contains(val)) {//去重
                    set.add(new IntWritable(val.get()));
                }
            }
            java.util.Map map = calcDistance(key, userInfo, set);//得到以相似度排序后的 用户名编号->相似度 映射
            Set entry = map.entrySet();
            Iterator it;
            int i;
            //输出neighborsNum个最相似的用户名编号
            for (it = entry.iterator(), i = 0; it.hasNext() && i < neighborsNum; i++) {
                java.util.Map.Entry current = (java.util.Map.Entry) it.next();
                builder.append(String.format("%s:%.2f;", current.getKey(), current.getValue()));
            }
            builder.setLength(builder.length() - 1);
            context.write(key, new Text(builder.toString()));
        }
    }
}
