package cn.mykine.userbehavior.bean.mytest.groovy

class FootBallPlayer implements Player{

    @Override
    void play() {
        println("踢足球");
    }

    @Override
    String showMe(String name,Integer height ,Integer weight) {
        return "你好，我叫"+name+",我是个足球运动员,身高:"+height+"cm,体重:"+weight+"kg";
    }

}
