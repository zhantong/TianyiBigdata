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

        //String tmpPath=args[2];
        String pathFormatLabel="/tmp/outformatlabel";
        Configuration jobFormatLabelConf = new Configuration();
        Job jobFormatLabel = new Job(jobFormatLabelConf,"format label");
        jobFormatLabel.setJarByClass(FormatLabel.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobFormatLabel.setOutputKeyClass(Text.class);
        jobFormatLabel.setOutputValueClass(IntWritable.class);
        jobFormatLabel.setMapperClass(FormatLabel.Map.class);
        jobFormatLabel.setCombinerClass(FormatLabel.OneCombiner.class);
        jobFormatLabel.setReducerClass(FormatLabel.Reduce.class);
        jobFormatLabel.setInputFormatClass(TextInputFormat.class);
        jobFormatLabel.setOutputFormatClass(TextOutputFormat.class);
        jobFormatLabel.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobFormatLabel, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFormatLabel, new Path(pathFormatLabel));
        ControlledJob cJobFormatLabel=new ControlledJob(jobFormatLabelConf);
        cJobFormatLabel.setJob(jobFormatLabel);

        String pathFormatUser="/tmp/outformatuser";
        Configuration jobFormatUserConf=new Configuration();
        Job jobFormatUser=new Job(jobFormatUserConf,"format user");
        jobFormatUser.setJarByClass(FormatUser.class);
        jobFormatUser.setOutputKeyClass(Text.class);
        jobFormatUser.setOutputValueClass(IntWritable.class);
        jobFormatUser.setMapperClass(FormatUser.Map.class);
        jobFormatUser.setCombinerClass(FormatUser.OneCombiner.class);
        jobFormatUser.setReducerClass(FormatUser.Reduce.class);
        jobFormatUser.setInputFormatClass(TextInputFormat.class);
        jobFormatUser.setOutputFormatClass(TextOutputFormat.class);
        jobFormatUser.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobFormatUser, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFormatUser, new Path(pathFormatUser));
        ControlledJob cJobFormatUser=new ControlledJob(jobFormatUserConf);
        cJobFormatUser.setJob(jobFormatUser);

        Configuration jobSumUsersConf = new Configuration();
        Job jobSumUsers = new Job(jobSumUsersConf, "sum users");
        jobSumUsers.addCacheFile(URI.create(pathFormatUser+"/part-r-00000"));
        jobSumUsers.addCacheFile(URI.create(pathFormatLabel+"/part-r-00000"));
        jobSumUsers.setJarByClass(SumUsers.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobSumUsers.setOutputKeyClass(Text.class);
        jobSumUsers.setOutputValueClass(Text.class);
        jobSumUsers.setMapperClass(SumUsers.Map.class);
        jobSumUsers.setCombinerClass(SumUsers.SumCombiner.class);
        jobSumUsers.setPartitionerClass(SumUsers.NewPartitioner.class);
        jobSumUsers.setReducerClass(SumUsers.Reduce.class);
        jobSumUsers.setInputFormatClass(TextInputFormat.class);
        jobSumUsers.setOutputFormatClass(TextOutputFormat.class);
        jobSumUsers.setMapOutputKeyClass(Text.class);
        jobSumUsers.setMapOutputValueClass(IntWritable.class);
        jobSumUsers.setNumReduceTasks(2);
        FileInputFormat.addInputPath(jobSumUsers, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobSumUsers, new Path(args[1]));
        ControlledJob cJobSumUsers=new ControlledJob(jobSumUsersConf);
        cJobSumUsers.setJob(jobSumUsers);
        cJobSumUsers.addDependingJob(cJobFormatUser);
        cJobSumUsers.addDependingJob(cJobFormatLabel);

        Configuration jobInvertedIndexConf = new Configuration();
        Job jobInvertedIndex = new Job(jobInvertedIndexConf, "inverted index");
        jobInvertedIndex.setJarByClass(InvertedIndex.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        jobInvertedIndex.setOutputKeyClass(Text.class);
        jobInvertedIndex.setOutputValueClass(Text.class);
        jobInvertedIndex.setMapperClass(InvertedIndex.Map.class);
        jobInvertedIndex.setCombinerClass(InvertedIndex.SumCombiner.class);
        jobInvertedIndex.setReducerClass(InvertedIndex.Reduce.class);
        jobInvertedIndex.setInputFormatClass(TextInputFormat.class);
        jobInvertedIndex.setOutputFormatClass(TextOutputFormat.class);
        jobInvertedIndex.setMapOutputKeyClass(Text.class);
        jobInvertedIndex.setMapOutputValueClass(Text.class);
        jobInvertedIndex.setNumReduceTasks(2);
        FileInputFormat.addInputPath(jobInvertedIndex, new Path("output"));
        FileOutputFormat.setOutputPath(jobInvertedIndex, new Path("outputc"));
        ControlledJob cJobInvertedIndex=new ControlledJob(jobInvertedIndexConf);
        cJobInvertedIndex.setJob(jobInvertedIndex);
        cJobInvertedIndex.addDependingJob(cJobSumUsers);

        JobControl jc=new JobControl("My job control");
        jc.addJob(cJobFormatLabel);
        jc.addJob(cJobFormatUser);
        jc.addJob(cJobSumUsers);
        jc.addJob(cJobInvertedIndex);


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
