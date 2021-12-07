package com.aifurion.display.common;



/**
 * 失效字段枚举
 *
 * @author zzy
 */

public enum DisableEnum {

    /**
     * 启用
     */
    ENABLE(0, "启用"),
    /**
     * 禁用
     */
    DISABLE(1, "禁用");

    public Integer getValue() {
        return value;
    }

    public String getComment() {
        return comment;
    }

    DisableEnum(Integer value, String comment) {
        this.value = value;
        this.comment = comment;
    }

    private final Integer value;

    private final String comment;

}
