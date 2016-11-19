package nju.edu.cn;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import sun.rmi.runtime.Log;
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;

/**
 * Created by hadoop on 11/16/16.
 */
public class SNTriangleCountDriver {
    public static void main(String[] args) throws IOException
    {
        String[] input={"",args[1]+"/Data0"};
        input[0]=args[0];
        PreLine.main(input);
        input[0]=input[1]+"/part-r-00000";
        input[1]=args[1]+"/sum";
        GetCount.main(input);

        Configuration cf = GetConf();
        long startTime = System.currentTimeMillis();
        FileSystem fileSystem = FileSystem.get(cf);	//获得当前的文件系统
        FileStatus[] fileStatus = fileSystem.listStatus(input);	//获得三角形计数结果目录下的所有文件状态
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        Long totalCount = 0l;
        for(Path p: paths){ //列出三角形计数结果目录下的所有文件
            if(!p.toString().contains("part-r-")){
                continue;
            }
            //读取每个part-r-000xx文件下的数字并求和
            BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(p),"utf-8"));
            String line = br.readLine();
            while(line != null){
                if(!line.isEmpty()){
                    totalCount += Long.parseLong(line.split("\t")[0]);
                }
                line = br.readLine();
            }
            br.close();
        }
        HadoopVariables.deletePath(fileSystem, outputPath); //deletePath方法将判断输出目录是否存在，若存在，则删除
        //将三角形计数的最终结果输出
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileSystem.create(
                new Path(outputPath.toString() + "/part-r-00000"))));
        bw.write(String.format("%d\n", totalCount));
        bw.close();
        long endTime = System.currentTimeMillis();
        Log.println(String.format("Count total number in local master.\nUsed time: %dms", endTime - startTime));

    }

    /**
     * 使用FileWriter类写文本文件
     */
    public static void writeMethod1(String fileName,String text)
    {
        try
        {
            //使用这个构造函数时，如果存在kuka.txt文件，
            //则先把这个文件给删除掉，然后创建新的kuka.txt
            FileWriter writer=new FileWriter(fileName);
            writer.write(text);
            writer.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 功能：Java读取txt文件的内容 步骤：1：先获得文件句柄 2：获得文件句柄当做是输入一个字节码流，需要对这个输入流进行读取
     * 3：读取到输入流后，需要读取生成字节流 4：一行一行的输出。readline()。 备注：需要考虑的是异常情况
     *
     * @param filePath
     *            文件路径[到达文件:如： D:\aa.txt]
     * @return 将这个文件按照每一行切割成数组存放到list中。
     */
    public static String readTxtFileIntoStringArrList(String filePath)
    {
        String Txt = "";
        try
        {
//            String encoding = "utf-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists())
            { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file));// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;

                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    Txt = Txt + lineTxt;
                }
                bufferedReader.close();
                read.close();
            }
            else
            {
                System.out.println("找不到指定的文件");
            }
        }
        catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

        return Txt;
    }
}
