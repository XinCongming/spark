package com;

import java.util.stream.Stream;

public class Demo3 {
    public static void main(String[] args) {
        Stream<Integer> stream = Stream.of(1,2,3,4,5,6);

        Stream<Integer> stream2 = Stream.iterate(10, (x) -> x + 2).limit(6);
        stream2.forEach(System.out::println); // 0 2 4 6 8 10  从10开始递加2，共6个

        Stream<Double> stream3 = Stream.generate(Math::random).limit(2);
        stream3.forEach(System.out::println);

    }
}
