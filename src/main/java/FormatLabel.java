import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhantong on 16/3/7.
 */
public class FormatLabel {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String keyWords=line.split("\t")[2];
            StringTokenizer tokenizer = new StringTokenizer(keyWords,",");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
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
