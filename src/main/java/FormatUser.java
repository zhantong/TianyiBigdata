import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhantong on 16/3/9.
 */
public class FormatUser {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String user=line.split("\t")[0];
            context.write(new Text(user), one);
        }
    }
    public static class OneCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            context.write(key,one);
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int count=0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new IntWritable(count));
            count++;
        }
    }
}
