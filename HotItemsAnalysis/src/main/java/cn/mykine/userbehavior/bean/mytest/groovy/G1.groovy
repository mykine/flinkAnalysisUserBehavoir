package cn.mykine.userbehavior.bean.mytest.groovy

class G1 {
    static void main(String[] args) {
        println("hello groovy");
        def g2 = new G2();
        println("g2.add(1,2)="+g2.add(1,2))
    }
}
