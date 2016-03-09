import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhantong on 16/3/8.
 */
public class InvertedIndex {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] split=line.split("\t");
            String user=split[0];
            StringTokenizer tokenizer = new StringTokenizer(split[1],";");
            while (tokenizer.hasMoreTokens()) {
                String item=tokenizer.nextToken().split(":")[0];
                context.write(new Text(item), new Text(user));
            }
        }
    }
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            StringBuilder out=new StringBuilder();
            for(Text val:values){
                out.append(val.toString()+",");
            }
            out.setLength(out.length()-1);
            context.write(key,new Text(out.toString()));
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder out=new StringBuilder();
            for(Text val:values){
                out.append(val.toString()+",");
            }
            out.setLength(out.length()-1);
            context.write(key,new Text(out.toString()));
        }
    }
}
