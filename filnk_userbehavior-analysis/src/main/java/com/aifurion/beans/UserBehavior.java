package com.aifurion.beans;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 10:39
 */
public class UserBehavior {


    private Long userId;

    private Long itemId;

    private Long categoryId;

    private String behaviorTpye;

    private Long timestamp;


    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behaviorTpye='" + behaviorTpye + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehaviorTpye() {
        return behaviorTpye;
    }

    public void setBehaviorTpye(String behaviorTpye) {
        this.behaviorTpye = behaviorTpye;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public UserBehavior(Long userId, Long itemId, Long categoryId, String behaviorTpye,
                        Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaviorTpye = behaviorTpye;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }
}
