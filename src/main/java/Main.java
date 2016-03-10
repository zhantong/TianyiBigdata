import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
 * MapReduce配置程序
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String pathInput = args[0];//输入原始文件路径
        //FormatLabel 将原始文件中的访问网站类型关键字转换为数字编号
        String pathFormatLabel = args[1];//网站关键字->关键字编号 映射
        Configuration jobFormatLabelConf = new Configuration();
        Job jobFormatLabel = new Job(jobFormatLabelConf, "format label");
        jobFormatLabel.setJarByClass(FormatLabel.class);
        jobFormatLabel.setOutputKeyClass(Text.class);
        jobFormatLabel.setOutputValueClass(IntWritable.class);
        jobFormatLabel.setMapperClass(FormatLabel.Map.class);
        jobFormatLabel.setCombinerClass(FormatLabel.OneCombiner.class);
        jobFormatLabel.setReducerClass(FormatLabel.Reduce.class);
        jobFormatLabel.setInputFormatClass(TextInputFormat.class);
        jobFormatLabel.setOutputFormatClass(TextOutputFormat.class);
        jobFormatLabel.setNumReduceTasks(1);//Reducer个数为1避免输出多个文件
        FileInputFormat.addInputPath(jobFormatLabel, new Path(pathInput));
        FileOutputFormat.setOutputPath(jobFormatLabel, new Path(pathFormatLabel));
        ControlledJob cJobFormatLabel = new ControlledJob(jobFormatLabelConf);
        cJobFormatLabel.setJob(jobFormatLabel);
        //FormatUser 将原始文件中的用户名转换为数字编号
        String pathFormatUser = args[2];//用户名->用户名编号 映射
        Configuration jobFormatUserConf = new Configuration();
        Job jobFormatUser = new Job(jobFormatUserConf, "format user");
        jobFormatUser.setJarByClass(FormatUser.class);
        jobFormatUser.setOutputKeyClass(Text.class);
        jobFormatUser.setOutputValueClass(IntWritable.class);
        jobFormatUser.setMapperClass(FormatUser.Map.class);
        jobFormatUser.setCombinerClass(FormatUser.OneCombiner.class);
        jobFormatUser.setReducerClass(FormatUser.Reduce.class);
        jobFormatUser.setInputFormatClass(TextInputFormat.class);
        jobFormatUser.setOutputFormatClass(TextOutputFormat.class);
        jobFormatUser.setNumReduceTasks(1);//Reducer个数为1避免输出多个文件
        FileInputFormat.addInputPath(jobFormatUser, new Path(pathInput));
        FileOutputFormat.setOutputPath(jobFormatUser, new Path(pathFormatUser));
        ControlledJob cJobFormatUser = new ControlledJob(jobFormatUserConf);
        cJobFormatUser.setJob(jobFormatUser);
        //SumUsers 将用户访问的网站关键字和访问次数归为一行, 用户名和网站关键字由编号替代
        String pathSumUsers = args[3];// 用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;.... 映射
        Configuration jobSumUsersConf = new Configuration();
        Job jobSumUsers = new Job(jobSumUsersConf, "sum users");
        jobSumUsers.addCacheFile(URI.create(pathFormatUser + "/part-r-00000"));//用户名->用户名编号 映射
        jobSumUsers.addCacheFile(URI.create(pathFormatLabel + "/part-r-00000"));//网站关键字->关键字编号 映射
        jobSumUsers.setJarByClass(SumUsers.class);
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
        jobSumUsers.setNumReduceTasks(2);//多个Reducer能提高速度
        FileInputFormat.addInputPath(jobSumUsers, new Path(pathInput));
        FileOutputFormat.setOutputPath(jobSumUsers, new Path(pathSumUsers));
        ControlledJob cJobSumUsers = new ControlledJob(jobSumUsersConf);
        cJobSumUsers.setJob(jobSumUsers);
        cJobSumUsers.addDependingJob(cJobFormatUser);//需要FormatUser执行完
        cJobSumUsers.addDependingJob(cJobFormatLabel);//需要FormatLabel执行完
        //InvertedIndex 关键字编号->用户名编号 倒排索引
        String pathInvertedIndex = args[4];// 关键字编号->用户名编号1,用户名编号2,用户名编号3... 映射
        Configuration jobInvertedIndexConf = new Configuration();
        Job jobInvertedIndex = new Job(jobInvertedIndexConf, "inverted index");
        jobInvertedIndex.setJarByClass(InvertedIndex.class);
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
        FileInputFormat.addInputPath(jobInvertedIndex, new Path(pathSumUsers));
        FileOutputFormat.setOutputPath(jobInvertedIndex, new Path(pathInvertedIndex));
        ControlledJob cJobInvertedIndex = new ControlledJob(jobInvertedIndexConf);
        cJobInvertedIndex.setJob(jobInvertedIndex);
        cJobInvertedIndex.addDependingJob(cJobSumUsers);//需要SumUsers执行完
        //GroupToOne 将SumUsers得到的多个Reduce输出文件合并为一个
        String pathGroupSumUsers = args[5];// 用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;.... 映射
        Configuration jobGroupToOneConf = new Configuration();
        Job jobGroupToOne = new Job(jobGroupToOneConf, "group to one");
        jobGroupToOne.setJarByClass(GroupToOne.class);
        jobGroupToOne.setOutputKeyClass(Text.class);
        jobGroupToOne.setOutputValueClass(NullWritable.class);
        jobGroupToOne.setMapperClass(GroupToOne.Map.class);
        jobGroupToOne.setReducerClass(GroupToOne.Reduce.class);
        jobGroupToOne.setInputFormatClass(TextInputFormat.class);
        jobGroupToOne.setOutputFormatClass(TextOutputFormat.class);
        jobGroupToOne.setNumReduceTasks(1);
        FileInputFormat.addInputPath(jobGroupToOne, new Path(pathSumUsers));
        FileOutputFormat.setOutputPath(jobGroupToOne, new Path(pathGroupSumUsers));
        ControlledJob cJobGroupToOne = new ControlledJob(jobGroupToOneConf);
        cJobGroupToOne.setJob(jobGroupToOne);
        cJobGroupToOne.addDependingJob(cJobSumUsers);//需要SumUsers执行完


        //上述5个MapReduce任务组合为任务链
        JobControl jc = new JobControl("job control");
        jc.addJob(cJobFormatLabel);
        jc.addJob(cJobFormatUser);
        jc.addJob(cJobSumUsers);
        jc.addJob(cJobGroupToOne);
        jc.addJob(cJobInvertedIndex);

        //开始执行
        Thread th = new Thread(jc);
        th.start();
        while (true) {
            if (jc.allFinished()) {
                jc.stop();
                break;
            }
        }

        //这一步需要依赖的输出在上述任务链最后才生成, 不能加在上一任务链中
        String pathToFind = args[6];//需要寻找相似用户的用户列表
        //用户名编号(需要寻找相似用户的用户)->用户名编号1:相似度1;用户名编号2:相似度2;... (以相似度由大到小排序) 映射
        String pathNeighbors = args[7];
        Configuration jobNeighborsConf = new Configuration();
        Job jobNeighbors = new Job(jobNeighborsConf, "find closest neighbors");
        jobNeighbors.addCacheFile(URI.create(pathToFind));//需要寻找相似用户的用户列表
        // 用户名编号->关键字编号1:访问次数1;关键字编号2:访问次数2;.... 映射
        jobNeighbors.addCacheFile(URI.create(pathGroupSumUsers + "/part-r-00000"));
        jobNeighbors.setJarByClass(Neighbors.class);
        jobNeighbors.setOutputKeyClass(IntWritable.class);
        jobNeighbors.setOutputValueClass(Text.class);
        jobNeighbors.setMapperClass(Neighbors.Map.class);
        jobNeighbors.setReducerClass(Neighbors.Reduce.class);
        jobNeighbors.setMapOutputKeyClass(IntWritable.class);
        jobNeighbors.setMapOutputValueClass(IntWritable.class);
        jobNeighbors.setInputFormatClass(TextInputFormat.class);
        jobNeighbors.setOutputFormatClass(TextOutputFormat.class);
        jobNeighbors.setNumReduceTasks(2);//多个Reducer能提高速度
        FileInputFormat.addInputPath(jobNeighbors, new Path(pathInvertedIndex));
        FileOutputFormat.setOutputPath(jobNeighbors, new Path(pathNeighbors));
        ControlledJob cJobNeighbors = new ControlledJob(jobNeighborsConf);
        cJobNeighbors.setJob(jobNeighbors);

        JobControl jc2 = new JobControl("job 2 control");
        jc2.addJob(cJobNeighbors);

        //开始执行
        Thread th2 = new Thread(jc2);
        th2.start();
        while (true) {
            if (jc2.allFinished()) {
                jc2.stop();
                break;
            }
        }
    }
}
