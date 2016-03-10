import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.net.URI;
import java.util.Hashtable;
import java.util.StringTokenizer;

/**
 * 将用户访问的网站关键字和访问次数归为一行
 * 用户名和网站关键字由编号替代
 */
public class SumUsers {
    /**
     * Mapper
     * 读取 用户名->用户名编号 以及 网站关键字->关键字编号
     * 将原始文件以 用户名编号#关键字编号 访问次数 输出
     * 输入: key: LongWritable 当前行偏移距离    value: Text 当前行内容, 用户名+访问时间+访问网站类型关键字+访问次数
     * 输出: key: Text 用户名编号#关键字编号      value: IntWritable 访问次数
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Hashtable<String, String> formatUser = new Hashtable<>();//用户名->用户名编号
        private Hashtable<String, String> formatLabel = new Hashtable<>();//网站关键字->关键字编号

        /**
         * mapper setup
         * 读取 用户名->用户名编号 以及 网站关键字->关键字编号 关系
         *
         * @param context 上下文
         */
        public void setup(Mapper.Context context) {
            URI[] cacheFiles;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String line;
                String[] tokens;
                cacheFiles = context.getCacheFiles();//DistributedCache
                //用户名
                Path pathUser = new Path(cacheFiles[0]);
                FSDataInputStream fsinUser = fs.open(pathUser);
                DataInputStream inUser = new DataInputStream(fsinUser);
                BufferedReader readerUser = new BufferedReader(new InputStreamReader(inUser));
                while ((line = readerUser.readLine()) != null) {
                    tokens = line.split("\t", 2);
                    formatUser.put(tokens[0], tokens[1]);
                }
                readerUser.close();
                inUser.close();
                fsinUser.close();
                //网站关键字
                Path pathLabel = new Path(cacheFiles[1]);
                FSDataInputStream fsinLabel = fs.open(pathLabel);
                DataInputStream inLabel = new DataInputStream(fsinLabel);
                BufferedReader readerLabel = new BufferedReader(new InputStreamReader(inLabel));
                while ((line = readerLabel.readLine()) != null) {
                    tokens = line.split("\t", 2);
                    formatLabel.put(tokens[0], tokens[1]);
                }
                readerLabel.close();
                inLabel.close();
                fsinLabel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * map
         *
         * @param key     当前行偏移距离
         * @param value   当前行内容, 用户名+访问时间+访问网站类型关键字+访问次数
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String userID = split[0];
            int count = Integer.parseInt(split[3]);
            String keyWords = split[2];
            StringTokenizer tokenizer = new StringTokenizer(keyWords, ",");
            //每个访问的网站可能有多个关键字, 每个关键字都记录一次, 使用相同的访问次数
            while (tokenizer.hasMoreTokens()) {
                String item = tokenizer.nextToken();
                context.write(new Text(formatUser.get(userID) + "#" + formatLabel.get(item)), new IntWritable(count));
            }
        }
    }

    /**
     * Combiner
     * 合并mapper输出, 减少向reducer传输数据量
     * 输入: key: Text 用户名编号#关键字编号    value: Iterable<IntWritable> 相同key的value集合, 例如[10,25,5,8]
     * 输出: key: Text 不变                   value: IntWritable 访问次数累加和
     */
    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * combiner reduce
         *
         * @param key     用户名编号#关键字编号
         * @param values  相同key的value集合, 例如[10,25,5,8]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * Partitioner
     * 确保相同用户名编号在同一个reduce节点处理
     * 输入: key: 用户名编号#关键字编号
     * 输出: partition key: 用户名编号
     */
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        /**
         * partition
         *
         * @param key            用户名编号#关键字编号
         * @param value          访问次数
         * @param numReduceTasks 内部参数
         * @return 内部参数
         */
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];//提取用户名编号
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    /**
     * Reducer
     * 将用户访问的网站关键字和访问次数归为一行
     * 输入: key: Text 用户名编号#关键字编号    value: Iterable<IntWritable> 相同key的value集合, 例如[10,25,5,8]
     * 输出: key: Text 用户名编号              value: Text 关键字编号1:访问次数1;关键字编号2:访问次数2;....
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private String last = " ";
        private StringBuilder out = new StringBuilder();
        private String term;
        private String item;
        private int thresholdDown = 10;//访问次数阈值下界
        private int thresholdUp = 20000;//访问次数阈值上界

        /**
         * reduce
         *
         * @param key     用户名编号#关键字编号
         * @param values  相同key的value集合, 例如[10,25,5,8]
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("#");
            term = split[0];
            item = split[1];
            if (!term.equals(last)) {
                if (!last.equals(" ")) {
                    if (out.length() > 0) {//防止因不在阈值内丢弃而导致的value长度为0
                        out.setLength(out.length() - 1);
                        context.write(new Text(last), new Text(out.toString()));
                        out = new StringBuilder();
                    }
                }
                last = term;
            }
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //访问次数不在阈值范围内的丢弃
            if (sum > thresholdDown) {
                if (sum > thresholdUp) {
                    sum = thresholdUp;
                }
                out.append(item + ":" + sum + ";");
            }
        }

        /**
         * reducer cleanup
         * 最后一行处理
         *
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (out.length() > 0) {
                out.setLength(out.length() - 1);
                context.write(new Text(last), new Text(out.toString()));
            }
        }
    }
}
