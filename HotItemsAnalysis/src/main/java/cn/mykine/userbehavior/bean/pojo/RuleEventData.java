package cn.mykine.userbehavior.bean.pojo;

import lombok.Data;

/**
 * 规则变更事件
 * */
@Data
public class RuleEventData {

    /**
     * 用户uid
     * */
    private DataChangeTypeEnum changeType;

    /**
     * 事件人群信息
     */
    private MarketingRule marketingRule;


}
