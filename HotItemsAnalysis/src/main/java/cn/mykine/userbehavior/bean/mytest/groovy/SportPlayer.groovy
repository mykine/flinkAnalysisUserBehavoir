package cn.mykine.userbehavior.bean.mytest.groovy

class SportPlayer {
    Integer height;
    Integer weight;
    String name;

    SportPlayer(String name) {
        this.name = name
    }

    SportPlayer() {}

    void play() {
        println("锻炼~");
    }

    void play(String project) {
        println("锻炼项目："+project);
    }

    /**
     * 多个参数，使用invokeMethod反射调用对象方法时，使用Obeject[] args数组传入对应所有参数值
     * */
    String showMe(String name ,Integer height,Integer weight){
        return "，你好,我叫"+name+",我是个体育运动员,身高:"+height+"cm,体重:"+weight+"kg";
    }

}
