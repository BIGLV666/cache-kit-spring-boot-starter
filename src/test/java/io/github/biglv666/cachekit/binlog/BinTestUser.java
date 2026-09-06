package io.github.biglv666.cachekit.binlog;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/** binlog 失效测试实体：MySQL 中的 user_bin 表 */
@TableName("user_bin")
public class BinTestUser {

    @TableId
    private Long userId;
    private String username;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
