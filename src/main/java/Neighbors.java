import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by zhantong on 16/3/9.
 */
public class Neighbors {
    private static List<String> toFind=new ArrayList<>();
    private static HashMap<Integer, Integer> toMap(String str){
        HashMap<Integer,Integer> map=new HashMap<>();
        String[] items=str.split(";");
        for(String item:items){
            String[] split=item.split(":");
            map.put(Integer.parseInt(split[0]),Integer.parseInt(split[1]));
        }
        return map;
    }
    private static double deviation(Collection<Integer> collection){
        double sum=0f;
        for(Integer a:collection){
            sum+=a*a;
        }
        return Math.sqrt(sum);
    }
    private static java.util.Map calcDistance(IntWritable key,Hashtable<String,String> userInfo, Set<IntWritable> set){
        HashMap<String,Double> map=new HashMap<>();
        String left=key.toString();
        HashMap<Integer,Integer> mapA=toMap(userInfo.get(left));
        for(IntWritable item:set){
            String right=item.toString();
            HashMap<Integer,Integer> mapB=toMap(userInfo.get(right));
            map.put(right,calc(mapA,mapB));
        }
        return utils.sortByValue(map);
    }
    private static double calc(HashMap<Integer,Integer> mapA,HashMap<Integer,Integer> mapB){
        double sum=0f;
        for(int keyA:mapA.keySet()){
            if(mapB.containsKey(keyA)){
                sum+=mapA.get(keyA)*mapB.get(keyA);
            }
        }
        if(sum==0f){
            return 0f;
        }
        return sum / (deviation(mapA.values()) * deviation(mapB.values()));
    }
    private static boolean contain(String[] array,String item){
        for(String val:array){
            if(val.equals(item)){
                return true;
            }
        }
        return false;
    }
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void setup(Mapper.Context context){
            URI[] cacheFiles;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String line;
                cacheFiles = context.getCacheFiles();
                Path pathToFind=new Path(cacheFiles[0]);
                FSDataInputStream fsinToFind = fs.open(pathToFind);
                DataInputStream inToFind = new DataInputStream(fsinToFind);
                BufferedReader readerToFind=new BufferedReader(new InputStreamReader(inToFind));
                while ((line = readerToFind.readLine()) != null) {
                    toFind.add(line);
                }
                readerToFind.close();
                inToFind.close();
                fsinToFind.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] split=line.split("\t");
            String[] users=split[1].split(",");
            for(String item:toFind){
                if(contain(users,item)){
                    IntWritable first=new IntWritable(Integer.parseInt(item));
                    for(String second:users){
                        if(!second.equals(item)){
                            context.write(first,new IntWritable(Integer.parseInt(second)));
                        }
                    }
                }
            }
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        private Hashtable<String,String> userInfo=new Hashtable<>();
        String[] tokens;
        private final static int neighborsNum=20;
        public void setup(Reducer.Context context){
            URI[] cacheFiles;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String line;
                cacheFiles = context.getCacheFiles();
                Path pathToFind=new Path(cacheFiles[1]);
                FSDataInputStream fsinToFind = fs.open(pathToFind);
                DataInputStream inToFind = new DataInputStream(fsinToFind);
                BufferedReader readerToFind=new BufferedReader(new InputStreamReader(inToFind));
                while ((line = readerToFind.readLine()) != null) {
                    tokens = line.split("\t", 2);
                    userInfo.put(tokens[0], tokens[1]);
                }
                readerToFind.close();
                inToFind.close();
                fsinToFind.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Set<IntWritable> set=new HashSet<>();
            StringBuilder builder=new StringBuilder();
            for(IntWritable val:values){
                if(!set.contains(val)) {
                    set.add(new IntWritable(val.get()));
                }
            }
            java.util.Map map= calcDistance(key,userInfo,set);
            Set entry=map.entrySet();
            Iterator it;
            int i;
            for(it=entry.iterator(),i=0;it.hasNext() && i<neighborsNum;i++){
                java.util.Map.Entry current=(java.util.Map.Entry)it.next();
                builder.append(String.format("%s:%.2f;",current.getKey(),current.getValue()));
            }
            builder.setLength(builder.length()-1);
            context.write(key,new Text(builder.toString()));
        }
    }
}
