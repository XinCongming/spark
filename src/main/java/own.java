import java.lang.reflect.Method;

public class own {
    interface MathOperation{
        int method1(int a,int b);
    }

    int operator(int a,int b,MathOperation math){
        return math.method1(a,b);
    }

    public static void main(String[] args) {
//        MathOperation  math = (a,b) -> a+b;
//        own own = new own();
//        int resulte = own.operator(5, 2, math);
////        System.out.println(resulte);
//
//        int a=3,b=44;
//        int i = math.method1(a, b);
//        System.out.println(i);


    }
}
