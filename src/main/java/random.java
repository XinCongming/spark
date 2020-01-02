import java.io.FileWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class random {
    static Random r = new Random();
    public static ArrayList<String> movie = new ArrayList<String>();
    public static ArrayList<String> cinema = new ArrayList<String>();


    public static void main(String[] args) throws Exception {

        if (args ==null || args.length == 0){
            System.out.println("no args");
            System.exit(-1);
        }

        getCallLog(args[0]);
//            getCallLog();
    }
    //    public static void getCallLog() throws Exception {
    public static void getCallLog(String LogFile) throws Exception {

        FileWriter fw = new FileWriter(LogFile,true);
        String MovieName = null;
        String Cinema = null;



        while (true){
            String s1 = movie.get(r.nextInt(movie.size()));
            MovieName  = s1;

            String s2 = cinema.get(r.nextInt(cinema.size()));
            Cinema = s2;


            //分数
            Double score1 = r.nextDouble()*10;
            DecimalFormat decimalFormat = new DecimalFormat();
            decimalFormat.applyPattern("0.0");
            String score = decimalFormat.format(score1);



            //用户id
            Integer userid1 = r.nextInt(1000);
            DecimalFormat decimalFormat1 = new DecimalFormat();
            decimalFormat1.applyPattern("000");
            String userid = decimalFormat1.format(userid1);


            //时间
            int year = 2019;
            int mouth = 12;
            int day = r.nextInt(29)+1;
            int hours = r.nextInt(24);
            int min = r.nextInt(60);
            int sec =r.nextInt(60);
            Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR,year);
            c.set(Calendar.MONTH,mouth);
            c.set(Calendar.DAY_OF_MONTH,day);
            c.set(Calendar.HOUR_OF_DAY,hours);
            c.set(Calendar.MINUTE,min);

            c.set(Calendar.SECOND,sec);
            Date date = c.getTime();

            // 如果时间超过今天就重新qushijian取时间.
            Date now = new Date();
            if (date.compareTo(now) > 0) {
                continue ;
            }

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
            simpleDateFormat.applyPattern("yyyyMMdd-HH");
            String dateStr = simpleDateFormat.format(date);
            String log =userid+","+ MovieName + "," + Cinema + ","+ dateStr + ","+score;





            System.out.println(log);
            fw.write(log + "\r\n");
            fw.flush();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}


