package cn.mykine.userbehavior.bean.pojo;

import java.nio.ByteBuffer;

/**
 * 模拟规则用户人群
 * */
public class TestUser1 {
    // 主键id
    private Integer id;
    //规则名称
    private String ruleName;

    //人群bitmap二进制数据
    private byte[] uidsBitmap;

    //创建时间
    private String createDt;

    //修改时间
    private String updateDt;



    public TestUser1() {
    }

    public TestUser1(Integer id, String ruleName, byte[] uidsBitmap, String createDt, String updateDt) {
        this.id = id;
        this.ruleName = ruleName;
        this.uidsBitmap = uidsBitmap;
        this.createDt = createDt;
        this.updateDt = updateDt;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public byte[] getUidsBitmap() {
        return uidsBitmap;
    }

    public void setUidsBitmap(byte[] uidsBitmap) {
        this.uidsBitmap = uidsBitmap;
    }

    public String getCreateDt() {
        return createDt;
    }

    public void setCreateDt(String createDt) {
        this.createDt = createDt;
    }

    public String getUpdateDt() {
        return updateDt;
    }

    public void setUpdateDt(String updateDt) {
        this.updateDt = updateDt;
    }
}
