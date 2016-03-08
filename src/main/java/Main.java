import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

/**
 * Created by zhantong on 16/3/7.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        /*
        Configuration conf = new Configuration();
        Job job = new Job(conf, "format label");
        job.setJarByClass(FormatLabel.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(FormatLabel.Map.class);
        job.setCombinerClass(FormatLabel.OneCombiner.class);
        job.setReducerClass(FormatLabel.Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
        */
        Configuration conf = new Configuration();
        Job job = new Job(conf, "format label");
        job.addCacheFile(URI.create("hdfs://localhost:9000/user/zhantong/part-r-00000"));
        job.setJarByClass(FormatUsers.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(FormatUsers.Map.class);
        job.setCombinerClass(FormatUsers.SumCombiner.class);
        job.setPartitionerClass(FormatUsers.NewPartitioner.class);
        job.setReducerClass(FormatUsers.Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
