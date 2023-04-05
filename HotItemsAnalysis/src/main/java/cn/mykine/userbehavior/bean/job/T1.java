package cn.mykine.userbehavior.bean.job;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class T1 {
    public static void main(String[] args) throws IOException {
        long[] ruleTargetUids = new long[]{10001,20002,30003,50005};

        //将人群id转换成bitmap
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(ruleTargetUids);

        //将bitmap序列化到一个字节数组中
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        bitmap.serialize(dout);
        byte[] bitmapBytes = bout.toByteArray();
        String chs = "";
        for (int i = 0; i <bitmapBytes.length; i++) {
            String ch = String.valueOf((char)bitmapBytes[i]);
            chs+=ch;
        }
        String str = bitmapBytes.toString();

        System.out.println(chs);

//        String str = "AQEAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAQAAAP4BAAAAAQIEAAAAESciTjN1VcMBAAAAAAAAAAAAAAAAAAAA";
//
//        byte[] uidsBitmaps = str.getBytes();
//        //反序列化bitmap字节数组成bitmap对象
//        Roaring64Bitmap roaring64Bitmap = Roaring64Bitmap.bitmapOf();
//        roaring64Bitmap.deserialize(ByteBuffer.wrap(uidsBitmaps));
//        //判断用户是否在bitmap中
//        System.out.println("11111 是否在bitmap中:"+roaring64Bitmap.contains(11111));
//        System.out.println("30003 是否在bitmap中:"+roaring64Bitmap.contains(30003));
//        System.out.println("123 是否在bitmap中:"+roaring64Bitmap.contains(123));
//        System.out.println("20002 是否在bitmap中:"+roaring64Bitmap.contains(20002));
//        System.out.println("50005 是否在bitmap中:"+roaring64Bitmap.contains(50005));

    }
}
