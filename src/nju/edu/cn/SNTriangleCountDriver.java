package nju.edu.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Arrays;


/**
 * Created by hadoop on 11/16/16.
 */
public class SNTriangleCountDriver {
    public static void main(String[] args) throws IOException,Exception
    {
        String[] input={"",args[1]+"/Data0"};
        input[0]=args[0];
        PreLine.main(input);
        input[0]=input[1]+"/part-r-00000";
        input[1]=args[1]+"/sum";
        GetCount.main(input);
        input[0]=input[1];
        input[1]=args[1]+"/sum";
        run(input);

    }


    public static void run(String[] args) throws Exception {
        System.out.println("*******************");
        System.out.println(Arrays.toString(args));
        Configuration cf = new Configuration();
        // get hadf system
        FileSystem hdfsFileSystem = FileSystem.get(cf);

        Path reduceResultDir = new Path(args[0]);
        FileStatus[] fileStatuses = hdfsFileSystem.listStatus(reduceResultDir);
        Path[] reduceResult = FileUtil.stat2Paths(fileStatuses);
        long totalTriangle = 0;
        //list result file of triangles programming
        for (Path p : reduceResult) {
            if (!p.toString().contains("part-r-"))
                continue;


            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFileSystem.open(p), "utf-8"));
            String line = br.readLine();
            while (line != null) {
                if (!line.isEmpty()) {
                    totalTriangle += Long.parseLong(line.split("\t")[1]);
                }
                line = br.readLine();
            }
            br.close();
        }
        System.out.println("totalTriangle:\t"+totalTriangle);

        //output the finally result
        Path outPutPath = new Path(args[1]+"/finallyResult");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfsFileSystem.create(outPutPath)));
        bw.write(String.format("%d\n", totalTriangle));
        bw.close();

    }


}
