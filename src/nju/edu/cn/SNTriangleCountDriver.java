package nju.edu.cn;

/**
 * Created by hadoop on 11/16/16.
 */
public class SNTriangleCountDriver {
    public static void main(String[] args)
    {
        String[] input={"",args[1]+"/Data0"};
        input[0]=args[0];
        PreLine.main(input);
        input[0]=input[1]+"/part-r-00000";
        input[1]=args[1]+"/sum";
        GetCount.main(input);
    }
}
