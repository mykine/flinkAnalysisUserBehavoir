package cn.mykine.userbehavior.bean.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * 模拟规则
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingRule {
    // 主键id
    private Integer id;
    //规则名称
    private String ruleName;

    //人群bitmap
    private Roaring64Bitmap uidsBitmap;


}
