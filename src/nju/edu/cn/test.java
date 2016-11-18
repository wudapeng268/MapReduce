package nju.edu.cn;

import java.util.ArrayList;

/**
 * Created by Mr.Wu on 2016/11/16.
 */
public class test {
    public static void main(String[] args)
    {
        String[] abc="a,b,c,".split(",");
        ArrayList<String> tt=new ArrayList<String>();
        tt.add("123");
        tt.add("123");
        tt.add("8");
        tt = containTwo(tt);
        System.out.println(tt);

        // create an empty array list
        ArrayList<String> color_list = new ArrayList<String>();

        // use add() method to add values in the list
        color_list.add("White");
        color_list.add("Black");
        color_list.add("Red");

        // create an empty array sample with an initial capacity
        ArrayList<String> sample = new ArrayList<String>();

        // use add() method to add values in the list
        sample.add("Green");
        sample.add("Red");
        sample.add("White");

        // remove all elements from second list if it exists in first list
        sample.removeAll(color_list);

        System.out.println("First List :"+ color_list);
        System.out.println("Second List :"+ sample);

//        String[] aim = tt.toArray(new String[0]);
//
//        System.out.println(Arrays.toString(aim));
//        Arrays.sort(aim);
//        System.out.println(Arrays.toString(aim));
//        System.out.println("123".compareTo("123"));
    }
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
}
