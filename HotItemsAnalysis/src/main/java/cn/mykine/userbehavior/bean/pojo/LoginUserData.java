package cn.mykine.userbehavior.bean.pojo;

import lombok.Data;

@Data
public class LoginUserData {

    private String id;

    /**
     * 登录的时间(毫秒级)
     */
    private Long loginTime;

    /**
     * 是否是新用户:1是,0否
     */
    private Integer isNew;

    /**
     * ios设备信息,各大广告平台加密后传过来的值,一般加密后值
     */
    private String iosDeviceid;

    /**
     * 安卓串号,一般加密后值
     */
    private String imei;

    /**
     * 移动安全联盟提出的安卓系统移动终端设备补充标识,一般加密后值
     */
    private String oaid;

    /**
     * 安卓设备id,一般加密后值
     */
    private String androidId;


}
