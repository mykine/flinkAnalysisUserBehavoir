package cn.mykine.userbehavior.bean.pojo;

import lombok.Data;

@Data
public class LoginUserData {

    /**
     * 用户uid
     * */
    private String id;

    /**
     * 平台类型:1-广点通,2-应用宝,3-华为,4-粉丝通,5-小米,6-OPPO,7-VIVO,8-百度SEM
     */
    private Integer platform;

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

    /**
     * 广告id
     */
    private String adId;

    /**
     * 广告名称
     */
    private String adName;

}
