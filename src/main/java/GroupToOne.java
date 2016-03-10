import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 将多个Reducer得到的多个输出文件合并为一个
 */
public class GroupToOne {
    /**
     * Mapper
     * 读取文件后直接传递给Reducer
     * 输入: key: LongWritable 当前行偏移距离    value: Text 当前行内容
     * 输出: key: Text 当前行内容               value: NullWritable null
     */
    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
        /**
         * map
         *
         * @param key     当前行偏移距离
         * @param value   当前行内容
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    /**
     * Reducer
     * 不作处理
     * 输入: key: Text 行内容    value: NullWritable null
     * 输出: key: 不变           value: 不变
     */
    public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        /**
         * reduce
         *
         * @param key     行内容
         * @param values  null
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
