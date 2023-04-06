package cn.mykine.userbehavior.bean.pojo;

import lombok.Data;

/**
 * 用户行为事件
 * */
@Data
public class UserEventData {

    /**
     * 用户uid
     * */
    private long uid;

    /**
     * 事件名称
     */
    private String event;


}
