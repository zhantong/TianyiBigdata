import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 将原始文件中的用户名转换为数字编号
 */
public class FormatUser {
    /**
     * Mapper
     * 输入: key: LongWritable 当前行偏移距离    value: Text 当前行内容, 用户名+访问时间+访问网站类型关键字+访问次数
     * 输出: key: Text 用户名                   value: IntWritable 常量1
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        /**
         * map
         * 将用户名提取出来
         *
         * @param key     当前行偏移距离
         * @param value   当前行内容
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String user = line.split("\t")[0];
            context.write(new Text(user), one);
        }
    }

    /**
     * Combiner
     * 合并mapper输出, 减少向reducer传输数据量
     * 输入: key: Text 用户名    value: Iterable<IntWritable> 相同key的value集合, 例如[1,1,1,1]
     * 输出: key: 不变           value: IntWritable 常量1
     */
    public static class OneCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        /**
         * combiner reduce
         *
         * @param key     用户名
         * @param values  相同key的value集合, 例如[1,1,1,1]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }

    /**
     * Reducer
     * 为每个用户名指定一个编号
     * 输入: key: Text 用户名    value: Iterable<IntWritable> 相同key的value集合, 例如[1,1,1,1]
     * 输出: key: 不变           value: IntWritable 编号
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int count = 0;

        /**
         * reduce
         *
         * @param key     用户名
         * @param values  相同key的value集合, 例如[1,1,1,1]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new IntWritable(count));
            count++;
        }
    }
}
