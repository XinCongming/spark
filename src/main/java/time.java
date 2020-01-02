import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class time {
    public static void main(String[] args) throws IOException {

        FileOutputStream fileOutputStream = new FileOutputStream
                (new File("E:\\logs\\active.txt"));

        for(int o =0;o<1000;o++){
        Random random = new Random();
        int i = random.nextInt(500);
        String mid="mid_"+i;

        int i1 = random.nextInt(28)+1;
        int time=20191200+i1;

        String dd=mid+","+time+"\n";
        fileOutputStream.write(dd.getBytes());


        }
    }
}
