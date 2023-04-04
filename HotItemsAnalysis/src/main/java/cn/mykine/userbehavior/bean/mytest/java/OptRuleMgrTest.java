package cn.mykine.userbehavior.bean.mytest.java;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.*;

/**
 * 模拟规则管理后台管理规则数据到mysql
 * */
public class OptRuleMgrTest {
    public static void main(String[] args) throws IOException, SQLException {
        //模拟读取到es中的画像人群uid列表
        long[] ruleTargetUids = new long[]{10001,20002,30003,50005};

        //将人群id转换成bitmap
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(ruleTargetUids);

        //将bitmap序列化到一个字节数组中
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        bitmap.serialize(dout);
        byte[] bitmapBytes = bout.toByteArray();

        //将bitmap字节数据存储到mysql中对应的二进制类型的字段中
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.10.98:3306/marketing?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                "root", "jyIsTpYmq7%Z");
        String sql = "insert into test_user1(`rule_name`,`uids_bitmap`) values(?,?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1,"规则1");
        preparedStatement.setBytes(2,bitmapBytes);
        preparedStatement.execute();
        preparedStatement.close();
        conn.close();
        System.out.println("ok");
    }
}
