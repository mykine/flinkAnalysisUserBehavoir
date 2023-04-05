package cn.mykine.userbehavior.bean.mytest.java;

import cn.mykine.userbehavior.bean.mytest.groovy.Player;
import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;

/**
 * 模拟规则管理后台管理规则数据到mysql
 * */
@Slf4j
public class OptRuleQueryTest {
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.10.98:3306/marketing?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                "root", "jyIsTpYmq7%Z");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test_user1 limit 100;");
        //指向数据的指针移动，有数据时返回true
        while(resultSet.next()){
            log.info("开始处理数据");
            System.out.println("=================================================");
            String ruleName = resultSet.getString("rule_name");
            byte[] uidsBitmaps = resultSet.getBytes("uids_bitmap");
            //反序列化bitmap字节数组成bitmap对象
            Roaring64Bitmap roaring64Bitmap = Roaring64Bitmap.bitmapOf();
            roaring64Bitmap.deserialize(ByteBuffer.wrap(uidsBitmaps));
            //判断用户是否在bitmap中
            System.out.println("11111 是否在bitmap中:"+roaring64Bitmap.contains(11111));
            System.out.println("30003 是否在bitmap中:"+roaring64Bitmap.contains(30003));
            System.out.println("123 是否在bitmap中:"+roaring64Bitmap.contains(123));
            System.out.println("20002 是否在bitmap中:"+roaring64Bitmap.contains(20002));
            System.out.println("50005 是否在bitmap中:"+roaring64Bitmap.contains(50005));
        }

        statement.close();
        conn.close();
    }
}
