import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

/**
 * Created by zhantong on 16/3/7.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String tmpPath=args[2];
        Configuration jobAConf = new Configuration();
        Job jobA = new Job(jobAConf,"format label");
        jobA.setJarByClass(FormatLabel.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapperClass(FormatLabel.Map.class);
        jobA.setCombinerClass(FormatLabel.OneCombiner.class);
        jobA.setReducerClass(FormatLabel.Reduce.class);
        jobA.setInputFormatClass(TextInputFormat.class);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        jobA.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(tmpPath));
        ControlledJob cjobA=new ControlledJob(jobAConf);
        cjobA.setJob(jobA);

        Configuration jobBconf = new Configuration();
        Job jobB = new Job(jobBconf, "format label");
        jobB.addCacheFile(URI.create(tmpPath+"/part-r-00000"));
        jobB.setJarByClass(FormatUsers.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setMapperClass(FormatUsers.Map.class);
        jobB.setCombinerClass(FormatUsers.SumCombiner.class);
        jobB.setPartitionerClass(FormatUsers.NewPartitioner.class);
        jobB.setReducerClass(FormatUsers.Reduce.class);
        jobB.setInputFormatClass(TextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(IntWritable.class);
        jobB.setNumReduceTasks(2);
        FileInputFormat.addInputPath(jobB, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
        ControlledJob cjobB=new ControlledJob(jobBconf);
        cjobB.setJob(jobB);
        cjobB.addDependingJob(cjobA);

        JobControl jc=new JobControl("My job control");
        jc.addJob(cjobA);
        jc.addJob(cjobB);

        Thread th = new Thread(jc);
        th.start();
        while(true) {
            if (jc.allFinished()) {
                jc.stop();
                break;
            }
        }
    }
}
