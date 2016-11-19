package nju.edu.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 11/16/16.
 */
public class GetCount {
    public static class FindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //记录需要的边
//        static Map<String,Integer> path2int = new HashMap<String, Integer>();

        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{

            System.out.println(value.toString());
            if(!value.toString().contains(";"))//prestend block
                return;
            String[] keyAndValue=value.toString().split("\t");//old txt split as \t
            String[] Svalues = keyAndValue[1].split(";");
            String Skey = keyAndValue[0];
            int len=Svalues.length;
            int tt=0;
            String temp="";
            int count=0;
//            //统计可以满足的边,生成三角形
//            for(int i=0;i<len;i++)
//            {
//                temp=Skey+","+Svalues[i];
//                if(path2int.containsKey(temp))
//                {
//                    count+=path2int.get(temp);
//                }
//            }
            //output adj. that had
            for(int i=0;i<len;i++)
            {
                String OutputKey=Skey+","+Svalues[i];
                context.write(new Text(OutputKey),new IntWritable(0));
            }
            context.write(new Text(Skey),new IntWritable(count));
            //加入新的需要的边
            for(int i =0;i<len;i++)
            {
                for (int j=i+1 ;j<len;j++)
                {
                    String OutputKey=Svalues[i]+","+Svalues[j];
                    context.write(new Text(OutputKey),new IntWritable(1));
                }
            }
        }
    } //end map class

    public static class CombinerKey extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum=0;
            for(IntWritable val :values)
            {
                if (val.get()==0)
                    context.write(key,val);
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class SumCount extends Reducer<Text, IntWritable, Text, LongWritable> {//keyin valuein keyout valueout

        static long sum=0L;

        @Override
        protected void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sub_sum=0;
            boolean AddSum=false;
            for(IntWritable val:values)
            {
                if(val.get()==0)
                    AddSum=true;
                sub_sum+=val.get();

            }
            if (AddSum)
                sum+=sub_sum;
//            context.write(key,new LongWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            context.write(new Text("sum:"),new LongWritable(sum));
        }
    }

    public static void main(String[] args){
        try {
            System.out.println(args[0]+"   "+args[1]);
            Configuration configuration=new Configuration();
//            //这句话很关键
//            configuration.set("data.job.tracker", "192.168.1.15:9001");
//            //启动计算任务
            Job job=new Job(configuration, GetCount.class.getSimpleName());

            job.setJarByClass(GetCount.class);

            job.setMapperClass(FindMapper.class);
            job.setCombinerClass(CombinerKey.class);
            job.setReducerClass(SumCount.class);

            job.setNumReduceTasks(30);

            //设置映射Map输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce规约输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //设置输入和输出目录
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true)?0:1);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }//end main
}
