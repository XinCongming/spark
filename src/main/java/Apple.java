
public class Apple {
    private String name;
    private String color;
    private double weight;

    int num = 5;

    public Apple(String name, String color, double weight) {
        this.name = name;
        this.color = color;
        this.weight = weight;
    }
//构造方法
    public Apple() {
        System.out.println(num);
        System.out.println("this is contruction");
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Apple{" +
                "name='" + name + '\'' +
                ", color='" + color + '\'' +
                ", weight=" + weight +
                '}';
    }

//静态方法
    public static int compareByWeight(Apple a1, Apple a2) {
        double diff = a1.getWeight() - a2.getWeight();
        return new Double(diff).intValue();
    }
//实例方法
    public int compareByWeight2(Apple a1, Apple a2) {
        double diff = a1.getWeight() - a2.getWeight();
        return new Double(diff).intValue();
    }
//类方法
    public int compareByWeight3(Apple other) {
        //this是当前类的apple，other是传进来的
        double diff = this.getWeight() - other.getWeight();
        return new Double(diff).intValue();
    }

}
