package com;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;



import java.util.function.Function;

public class Demo4 {
        public static void main(String[] args) throws IOException {
//      要求：将list最后输出 abc123 or a b c 1 2 3 即各单位独立，而不是abc一组，123一组
//      map是输出多个流，flatmap是将多个流输出成一个流
            List<String> list = Arrays.asList("a,b,c", "1,2,3");
            Stream<String> stream = list.stream();

            //objectStream中含有6个元素，因为flagmap压平
            Stream<Object> objectStream = stream.flatMap(new Function<String, Stream<?>>() {
                @Override
                public Stream<?> apply(String s) {
                    //1去逗号   2、转成Stream
//                    String[] split = s.split(",");
                    String[] split = s.split(",");
//                    String s1 = s.replaceAll(",", "");
//                Stream<String> split1 = Stream.of(split);
                    Stream<String> stream1 = Arrays.stream(split);
                    return stream1;
                }
            });
            List<Object> collect = objectStream.collect(Collectors.toList());
            System.out.println(objectStream.count());
            System.out.println(collect.size());
            collect.forEach(System.out::print);

            System.out.println();
            System.out.println("------------------");
            //stringStream中含有2个元素，因为list集合有两个元素，map不具备压平机制
            Stream<String> stringStream = list.stream().map(new Function<String, String>() {

                @Override
                public String apply(String s) {
                    StringBuffer sb = new StringBuffer();
                    String[] split = s.split(",");
                    for (String s1 : split) {
                        sb.append(s1);
                    }
                    return sb.toString();
                }
            });

            List<String> collect2 = stringStream.collect(Collectors.toList());
            System.out.println(collect2.size());
            collect2.forEach(System.out::print);

            System.out.println();
            System.out.println("------------------");
            //将map输出集合中两个数组，分别转换流再转换成数组遍历输出
            List<String[]> collect1 = list.stream().map(p -> p.split(",")).collect(Collectors.toList());
            System.out.println(collect1.size());
            collect1.forEach(s -> Arrays.stream(s).collect(Collectors.toList()).forEach(System.out::print));

        }
}
