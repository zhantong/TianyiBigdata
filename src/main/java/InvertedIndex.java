import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 生成倒排索引
 */
public class InvertedIndex {
    /**
     * Mapper
     * 提取关键字和用户名,转换为编号并输出
     * 输入: key: LongWritable 当前行偏移距离    value: Text 当前行内容, 用户名编号+关键字编号1:访问次数1;关键字编号2:访问次数2;....
     * 输出: key: Text 关键字编号               value: Text 用户名编号
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * map
         *
         * @param key     当前行偏移距离
         * @param value   当前行内容, 用户名编号+关键字编号1:访问次数1;关键字编号2:访问次数2;....
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String user = split[0];
            StringTokenizer tokenizer = new StringTokenizer(split[1], ";");
            while (tokenizer.hasMoreTokens()) {
                String item = tokenizer.nextToken().split(":")[0];
                context.write(new Text(item), new Text(user));
            }
        }
    }

    /**
     * Combiner
     * 由于用户名编号没有重复,这样合并在reduce时不会造成用户名重复
     * 输入: key: Text 关键字编号    value: Iterable<Text> 相同key的value集合, 例如[用户名编号1,用户名编号2,用户名编号3]
     * 输出: key: 不变              value: 用户名编号1,用户名编号2,用户名编号3...
     */
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        /**
         * 合并mapper输出, 减少向reducer传输数据量
         *
         * @param key     关键字编号
         * @param values  相同key的value集合, 例如[用户名编号1,用户名编号2,用户名编号3]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for (Text val : values) {
                out.append(val.toString() + ",");
            }
            out.setLength(out.length() - 1);//删除最后的","
            context.write(key, new Text(out.toString()));
        }
    }

    /**
     * Reducer
     * 合并相同关键字编号的用户名编号
     * 输入: key: Text 关键字编号    value: Iterable<Text> 相同key的value集合, 例如["用户名编号1,用户名编号2,用户名编号3","用户名编号4,用户名编号5"]
     * 输出: key: 不变              value: 用户名编号1,用户名编号2,用户名编号3...
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        /**
         * reduce
         *
         * @param key     关键字编号
         * @param values  相同key的value集合, 例如["用户名编号1,用户名编号2,用户名编号3","用户名编号4,用户名编号5"]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for (Text val : values) {
                out.append(val.toString() + ",");
            }
            out.setLength(out.length() - 1);//删除最后的","
            context.write(key, new Text(out.toString()));
        }
    }
}
