package io.github.biglv666.cachekit.model;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/** 测试实体：MyBatis-Plus 注解元数据（验证零自有注解接入） */
@TableName("t_mp_user")
public class MpUserEntity {

    @TableId
    private Long userId;
    private String userName;

    public MpUserEntity() {
    }

    public MpUserEntity(Long userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MpUserEntity that)) {
            return false;
        }
        return java.util.Objects.equals(userId, that.userId)
                && java.util.Objects.equals(userName, that.userName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(userId, userName);
    }
}
