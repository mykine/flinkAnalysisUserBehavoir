package cn.mykine.userbehavior.bean.pojo;

/**
 * mysql数据变更类型
 * */
public enum DataChangeTypeEnum {
    INSERT(1,"新增"),
    UPDATE(2,"修改"),
    DELETE(3,"删除");

    private int type;
    private String desc;

    DataChangeTypeEnum(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }

}
