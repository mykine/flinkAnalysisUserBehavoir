package cn.mykine.userbehavior.bean.mytest.java;

import cn.mykine.userbehavior.bean.mytest.groovy.Player;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.sql.*;

/**
 * 动态加载groovy代码并通过反射实例化对象调用其方法
 * */
public class DynamicCallGroovyCode2 {
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException {
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.10.98:3306/marketing?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
                "root", "jyIsTpYmq7%Z");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test_code1 where id > 1 limit 100;");
        //指向数据的指针移动，有数据时返回true
        while(resultSet.next()){
            System.out.println("=================================================");
            String className = resultSet.getString("class_name");
            String groovyCode = resultSet.getString("groovy_code");
            System.out.println("类名："+className);
            System.out.println("groovy代码："+groovyCode);
            System.out.println("=======动态实例化并执行代码=========");
            //groovy类加载器
            GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
            //加载代码
            Class gClass = groovyClassLoader.parseClass(groovyCode);
            //反射调用无参构造函数实例化成都实现的接口对象
            Player player = (Player)gClass.newInstance();
            player.play();

            String name = "老梅";
            Integer height = 200;
            Integer weight = 160;

            String showMeRes = player.showMe(name,height,weight);
            System.out.println("showMeRes="+showMeRes);
        }


        conn.close();
    }
}
