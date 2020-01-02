
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class LambdaDemo2 {

    public LambdaDemo2(){
        System.out.println("当前构造方法");
    }
    public static void main(String[] args) {
        //静态方法引用
        Apple apple1 = new Apple("红富士", "Red", 280);
        Apple apple2 = new Apple("冯心", "Yello", 470);
        Apple apple3 = new Apple("大牛", "Red", 320);
        Apple apple4 = new Apple("小小", "Green", 300);
        List<Apple> appleList = Arrays.asList(apple1, apple2, apple3, apple4);
        //lambda 表达式形式
//            appleList.sort((Apple a1, Apple a2) -> {
//                return new Double(a1.getWeight() - a2.getWeight()).intValue();
//            });

        //静态方法引用形式（可以看出引用方法比上面的更加简单
        appleList.sort(Apple::compareByWeight);

//        appleList.forEach(apple -> System.out.println(apple));

        //实例方法引用
        Apple comparator = new Apple();
        appleList.sort(comparator::compareByWeight2);
//        appleList.forEach(apple -> System.out.println(apple));

        //类方法引用
        appleList.sort(Apple::compareByWeight3);
//        appleList.forEach(apple -> System.out.println(apple));

        //构造方法引用
        Supplier<Apple> apple = Apple::new;

        //当前类需调用get
        Supplier<LambdaDemo2> dangqian = LambdaDemo2::new;
        dangqian.get();
    }

}


