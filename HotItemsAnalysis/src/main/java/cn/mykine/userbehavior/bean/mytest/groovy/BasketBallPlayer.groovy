package cn.mykine.userbehavior.bean.mytest.groovy

class BasketBallPlayer implements Player{
    @Override
    void play() {
        println("打篮球");
    }

    @Override
    String showMe(int height ,int weight) {
        return "我是个篮球运动员,身高:"+height+"cm,体重:"+weight+"kg";
    }
}
