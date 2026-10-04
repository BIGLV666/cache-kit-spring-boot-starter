package io.github.biglv666.cachekit.binlog;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/** 租户键段还原集成测试实体：表带 tenant_id 列，键形如 {@code 租户:tenant_bin_order:id} */
@TableName("tenant_bin_order")
public class TenantBinOrder {

    @TableId
    private Long id;
    private String tenantId;
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
