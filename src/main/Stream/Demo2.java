

import com.xin.Person;

import java.util.*;
import java.util.stream.Collectors;

public class Demo2 {
    public static void main(String[] args) {
        List<Person> persionList = new ArrayList<Person>();
        persionList.add(new Person(1,"张三","男",38));
        persionList.add(new Person(2,"小小","女",2));
        persionList.add(new Person(3,"李四","男",65));
        persionList.add(new Person(4,"王五","女",20));
        persionList.add(new Person(5,"赵六","男",38));
        persionList.add(new Person(6,"大大","男",65));

        //1、只取出该集合中所有姓名组成一个新集合
        List<String> nameList=persionList.stream().map(Person::getName).collect(Collectors.toList());
        nameList.forEach(System.out::println);

//        2、只取出该集合中所有id组成一个新集合    boxed：数值流转换为流   mapToInt(T -> int) : return IntStream
        List<Integer> idList=persionList.stream().mapToInt(Person::getId).boxed().collect(Collectors.toList());
        List<Integer> idList2=persionList.stream().map(Person::getId).collect(Collectors.toList());
        System.out.println(idList.toString());

        //3、list转map，key值为id，value为Person对象-->Person：Person or person:peron  p大小写都行
        Map<Integer, Person> personmap = persionList.stream().collect(Collectors.toMap(Person::getId,Person -> Person));
        System.out.println(personmap.toString());

        //4、list转map，key值为id，value为name
        Map<Integer, String> namemap = persionList.stream().collect(Collectors.toMap(Person::getId, Person::getName));
        System.out.println(namemap.toString());

        //5、进行map集合存放，key为age值 value为Person对象 它会把相同age的对象放到一个集合中
        Map<Integer, List<Person>> ageMap = persionList.stream().collect(Collectors.groupingBy(Person::getAge));
        System.out.println(ageMap.toString());

        //6、获取最小年龄
        Integer ageMin = persionList.stream().mapToInt(Person::getAge).min().getAsInt();
        System.out.println("最小年龄为: "+ageMin);

        //7、获取最大年龄
        Integer ageMax = persionList.stream().mapToInt(Person::getAge).max().getAsInt();
        System.out.println("最大年龄为: "+ageMax);

        //8、集合年龄属性求和
        Integer ageAmount = persionList.stream().mapToInt(Person::getAge).sum();
        System.out.println("年龄总和为: "+ageAmount);

        //9.求年纪大于20岁的人数   p -> p.getAge()
        long count = persionList.stream().filter(person -> person.getAge() > 20).count();
        System.out.println(count);

        //10、求年纪大于20岁、性别为男的人数   p -> p.getGender()必须放在前面
        long count1 = persionList.stream().filter(p -> p.getAge() > 20).filter(p -> p.getGender().equals("男")).count();
        System.out.println(count1);

        //11、sorted相关例子
        String[] arr={"scsa","bi","a","cd","ca156165"};
        //按照字典姓名排序
        Object[] objects = Arrays.stream(arr).sorted((String o1, String o2) -> o1.length() - o2.length()).toArray();
        Object[] objects1 = Arrays.stream(arr).sorted().toArray();
        //姓名长度排序
        Object[] objects2 = Arrays.stream(arr).sorted(Comparator.comparing(String::length)).toArray();
        /**
         * 倒序
         * reversed(),java8泛型推导的问题，所以如果comparing里面是非方法引用的lambda表达式就没办法直接使用reversed()
         * Comparator.reverseOrder():也是用于翻转顺序，用于比较对象（Stream里面的类型必须是可比较的）
         * Comparator. naturalOrder()：返回一个自然排序比较器，用于比较对象（Stream里面的类型必须是可比较的）
         * */
        Object[] objects3 = Arrays.stream(arr).sorted(Comparator.comparing(String::length).reversed()).toArray();
        for(Object ob:objects){
            System.out.println(ob);
        }
        Arrays.stream(arr).sorted(Comparator.reverseOrder()).forEach(System.out::println);
        //输出：bc、abcd、abc、a
        Arrays.stream(arr).sorted(Comparator.naturalOrder()).forEach(System.out::println);
        //输出：a、abc、abcd、b

        //先按照首字母排序,之后按照String的长度排序  com1 自定义静态方法
        Arrays.stream(arr).sorted(Comparator.comparing(Demo2::com1).thenComparing(String::length)).forEach(System.out::println);
        //输出：a、abc、abcd、bc

        //1、找到年 龄最小的岁数
        Collections.sort(persionList, (x, y) -> x.getAge()-(y.getAge()));
        Integer age = persionList.get(0).getAge();
        System.out.println("年龄最小的有:" + age);
        //输出：年龄最小的有:2

        //2、找到年龄最小的姓名
        String name = persionList.stream()
                .sorted(Comparator.comparingInt(x -> x.getAge()))
                .findFirst()   //取第一个数据
                .get().getName();
        System.out.println("年龄最小的姓名:" + name);
        //输出：年龄最小的姓名:小小
    }
    public static char com1(String x){
        return x.charAt(0);
    }
}
