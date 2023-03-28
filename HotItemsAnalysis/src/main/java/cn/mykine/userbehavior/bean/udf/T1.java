package cn.mykine.userbehavior.bean.udf;

import java.text.SimpleDateFormat;
import java.util.Date;

public class T1 {
    public static void main(String[] args) {
//        String format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+0800")
//                .format(new Date(1679649565123L));
//        System.out.println("format="+format);
//        System.out.println(new Date(1));

        String format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                .format(1679846773145L);
        System.out.println("format="+format);
    }
}
