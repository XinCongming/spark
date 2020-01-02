import javax.xml.bind.SchemaOutputResolver;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class Demo1 {
    public static void main(String[] args) {
//        Random random = new Random();
//        random.ints().limit(5).forEach(System.out::println);
//        System.out.println("--------------------------------");
//        random.ints().limit(5).forEach(x -> System.out.println(x));

//        foreach
//        List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd","", "jkl");
//        List<String> filtered = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());
//        filtered.forEach(System.out::println);

//        List<Integer> numbers = Arrays.asList(3, 2, 2, 3, 7, 3, 5);
//        // 获取对应的平方数 map( i -> i*i) i表示每个元素 map类似于mapreduce阶段map distinct去重
//        List<Integer> squaresList = numbers.stream().map( i -> i*i).distinct().sorted().collect(Collectors.toList());
//        squaresList.forEach(System.out::println);

//        List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd","", "jkl");
//        // 获取空字符串的数量
//        long count = strings.parallelStream().filter(string -> string.isEmpty()).count();
//        System.out.println(count);

        List<String>strings = Arrays.asList("abc", "", "bc", "efg", "abcd","", "jkl");
        //Collectors.toList()  输出为一个list
        List<String> filtered = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());

        System.out.println("筛选列表: " + filtered);
        //Collectors.joining(", ") 将元素合并，以，隔开
        String mergedString = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.joining(", "));
        System.out.println("合并字符串: " + mergedString);
    }
}
