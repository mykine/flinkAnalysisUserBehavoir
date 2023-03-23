package cn.mykine.userbehavior.bean.pojo;

import lombok.Data;

import java.util.Date;

@Data
public class AdClickDataDo {

    private Long id;

    /**
     * 平台类型:1-广点通,2-应用宝,3-华为,4-粉丝通,5-小米,6-OPPO,7-VIVO,8-百度SEM
     */
    private Integer platform;

    /**
     * 点击id
     */
    private String clickId;

    /**
     * 点击时间(毫秒级)
     */
    private Long clickTime;

    /**
     * 设备id,不区分客户端类型的设备id,一般加密后值
     */
    private String muid;

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
     * 推广计划id
     */
    private String campainId;

    /**
     * 单元id
     */
    private String unitId;

    /**
     * 广告id
     */
    private String adId;

    /**
     * 广告名称
     */
    private String adName;

    /**
     * 素材id
     */
    private String materialId;

    /**
     * 投放渠道名称
     */
    private String channel;

    /**
     * 代理商账号id
     */
    private String accountId;

    /**
     * 进度
     */
    private String progress;

    /**
     * 激活时的用户设备信息json
     */
    private String activeDevice;

    /**
     * 激活时间(毫秒级)
     */
    private Long activeTime;

    /**
     * 状态:0-默认,1-已回传激活,2-已回传次留,
     */
    private Integer status;

    /**
     * 公司客户端id
     */
    private Long cid;

    /**
     * 回调参数
     */
    private String callbackParam;


    /**
     * 包名
     */
    private String pkg;

    /**
     * 字符串预留字段
     */
    private String paramStr;


    /**
     * 整数预留字段
     */
    private Integer paramInt;

    /**
     * 扩展信息, 当前表结构不满足时存放json
     */
    private String extra;

    /**
     * 创建时间
     */
    private Date createDt;

    /**
     * 更新时间
     */
    private Date updateDt;


}
