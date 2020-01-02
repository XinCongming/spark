import org.apache.avro.TestAnnotation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

public class LambdaDemo {
    public static void main(String[] args) {
        String[] strs = {"zhangsan","lisi","wangwu","zhaoliu"};

        //排序
//        Arrays.sort(strs);

//        Arrays.sort(strs, new Comparator<String>() {
//            @Override
//            public int compare(String o1, String o2) {
//                return o1.compareTo(o2);
//            }
//        });

//        Arrays.sort(strs,(o1,o2) -> o1.compareTo(o2));       //lambda
//        Arrays.sort(strs,(String o1,String o2) -> o1.length()-(o2.length()));
//
//        for(String s : strs){
//            System.out.println(s);
//        }

        //       new Thread(new Runnable() {
//           @Override
//           public void run() {
//               System.out.println("this is xiancheng!");
//           }
//       }).start();

//        new Thread( () -> System.out.println("this is lambda thread")).start();
    }


    @Test
    public void demo1(){

    }
}
