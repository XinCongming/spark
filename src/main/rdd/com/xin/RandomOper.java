package com.xin;

import java.util.Random;

public class RandomOper {
    public static void main(String[] args) {
        Random random = new Random(10);
        for(int i=0;i<10;i++){
            System.out.println(random.nextInt());
        }
        System.out.println("-------------------");
//      重新new random
        random = new Random(10);
        for(int i=0;i<10;i++){
            System.out.println(random.nextInt());
        }
//        你会发现输出的值一样，因为生成random算法一样，根据输入的种子决定，真正的随机数seed=System.currentTimeMillis()
    }
}
