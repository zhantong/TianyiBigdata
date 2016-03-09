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
        jobB.setJarByClass(SumUsers.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setMapperClass(SumUsers.Map.class);
        jobB.setCombinerClass(SumUsers.SumCombiner.class);
        jobB.setPartitionerClass(SumUsers.NewPartitioner.class);
        jobB.setReducerClass(SumUsers.Reduce.class);
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

        Configuration jobCconf = new Configuration();
        Job jobC = new Job(jobCconf, "inverted index");
        jobC.setJarByClass(InvertedIndex.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(Text.class);
        jobC.setMapperClass(InvertedIndex.Map.class);
        jobC.setCombinerClass(InvertedIndex.SumCombiner.class);
        jobC.setReducerClass(InvertedIndex.Reduce.class);
        jobC.setInputFormatClass(TextInputFormat.class);
        jobC.setOutputFormatClass(TextOutputFormat.class);
        jobC.setMapOutputKeyClass(Text.class);
        jobC.setMapOutputValueClass(Text.class);
        jobC.setNumReduceTasks(2);
        FileInputFormat.addInputPath(jobC, new Path("output"));
        FileOutputFormat.setOutputPath(jobC, new Path("outputc"));
        ControlledJob cjobC=new ControlledJob(jobCconf);
        cjobC.setJob(jobC);
        cjobB.addDependingJob(cjobA);
        cjobC.addDependingJob(cjobB);
        JobControl jc=new JobControl("My job control");
        jc.addJob(cjobA);
        jc.addJob(cjobB);
        jc.addJob(cjobC);

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
