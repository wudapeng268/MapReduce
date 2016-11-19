package nju.edu.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

//import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;

/**
 * Created by Snow on 16-10-31.
 *
 * this class is used to give document list by given word
 *
 */
public class PreLine {

    //reference : http://www.cnblogs.com/liqizhou/archive/2012/05/15/2501019.html
    public static class ExchangeMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable offset, Text values, Context context) throws IOException,InterruptedException{
            String[] t=values.toString().split(" ");
            LongString Svalue =new LongString(t[1]);
            LongString Skey = new LongString(t[0]);
            if (Skey.equals(Svalue))
                return;
            if (Skey.compareTo(Svalue)>0) {
                LongString temp = Svalue;
                Svalue = Skey;
                Skey = temp;
            }
            context.write(new Text(Skey.value+""),new Text(Svalue.value+""));

        }
    } //end map class

    //归并成单源的路径 e.g  1->2;3,5,7,8
    public static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {

        private Set<String> result = new HashSet<String>();
        private Text currentWord = new Text(" ");
        private Text keyTerm = new Text();

        @Override
        protected void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //getkey
            TreeSet<LongString> valueSet = new TreeSet<LongString>();
            for(Text value: values){
                valueSet.add(new LongString(value.toString()));
            }
            Iterator<LongString> iter = valueSet.iterator();
            StringBuilder sb = new StringBuilder();
            while(iter.hasNext()){
                sb.append(iter.next().value);{
                    sb.append(";");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            context.write(key, new Text(sb.toString()));
//            System.out.println(sb.toString());
//            String tempKey = key.toString();
//            keyTerm.set(tempKey);
//            if(!currentWord.equals(keyTerm) && !currentWord.equals(" ")){
//                // need to reset var
//                StringBuffer out = new StringBuffer();
//                for (String pos:result){
//                    out.append(pos+";");
//                }
//
//                context.write(currentWord, new Text(out.toString()));
//                result = new HashSet<String>();
//            }
//            currentWord.set(keyTerm);
//            for(Text val:values)
//            {
//                result.add(val.toString());
//            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuffer out = new StringBuffer();
            for (String pos:result){
                out.append(pos+";");
            }

            context.write(currentWord, new Text(out.toString()));
        }
    }

    public static class AndInvertedIndexReduce extends Reducer<Text, Text, Text, Text> {

        private ArrayList<String> result = new ArrayList<String>();
        private Text currentWord = new Text(" ");
        private Text keyTerm = new Text();

        @Override
        protected void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //getkey


            TreeSet<LongString> valueSet = new TreeSet<LongString>();
            String tempKey = key.toString();
            keyTerm.set(tempKey);
            currentWord.set(keyTerm);
            for(Text val:values)
            {
                result.add(val.toString());
            }

            result = containTwo(result);

            for(String s : result)
            {
                valueSet.add(new LongString(s));
            }


            StringBuffer out = new StringBuffer();
            for (LongString pos : valueSet){
                out.append(pos.value+";");
            }

            context.write(currentWord, new Text(out.toString()));
            result = new ArrayList<String>();
        }
        //set can get num where occur one or two
        //remove can get the num occur only one
        private static ArrayList<String> containTwo(ArrayList<String> list)
        {
            ArrayList<String> ans = new ArrayList<String>();
            int len=list.size();
            for(int i=0;i<len;i++)
            {
                for (int j=i+1;j<len;j++)
                {
                    if (list.get(j).equals(list.get(i))) {
                        ans.add(list.get(i));
                        break;
                    }
                }
            }
            return ans;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuffer out = new StringBuffer();
            for (String pos:result){
                out.append(pos+";");
            }

            context.write(currentWord, new Text(out.toString()));
        }
    }



    public static void main(String[] args){
        try {
            Configuration configuration=new Configuration();
//            //这句话很关键
//            configuration.set("data.job.tracker", "192.168.1.15:9001");
//            //启动计算任务
            Job job=new Job(configuration, PreLine.class.getSimpleName());

            job.setJarByClass(PreLine.class);

            job.setMapperClass(ExchangeMapper.class);
//            job.setReducerClass(AndInvertedIndexReduce.class);
            job.setReducerClass(InvertedIndexReduce.class);
            job.setNumReduceTasks(30);

            //设置映射Map输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //设置reduce规约输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置输入和输出目录
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);//wait for job

//            System.exit(job.waitForCompletion(true)?0:1);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }//end main
}
