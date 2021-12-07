package com.aifurion.display.common;

/**
 * 公共返回错误码
 *
 * @author zzy
 */
public enum BusCode implements IBusCode {

    /**
     * 0,成功
     */
    SUCCESS(0, "成功"),


    /**
     * 1101,无效token
     */
    TOKEN_INVALID(1101, "无效token"),

    /**
     * 1102,token过期
     */
    TOKEN_EXPIRATION(1102, "token过期"),

    /**
     * 1201,权限不足
     */
    PERMISSION_DENIED(1201, "权限不足"),

    /**
     * 1301，短信发送失败
     */
    SEND_SMS_ERROR(1301, "短信发送失败"),

    /**
     * 1302，短信已发送，请勿重复发送
     */
    SEND_SMS_REPEAT(1302, "短信已发送，请勿重复发送"),

    /**
     * 1601,用户不存在
     */
    USER_NOT_FOUND(1601, "用户不存在"),
    /**
     * 1602,用户ID不能为空
     */
    USER_ID_IS_NULL(1602, "用户ID不能为空"),
    /**
     * 1603,密码错误
     */
    USER_PASSWORD_ERROR(1603, "密码错误"),

    /**
     * 1611,角色不存在
     */
    ROLE_NOT_FOUND(1611, "角色不存在"),
    /**
     * 1612,角色编码已存在
     */
    ROLE_CODE_EXISTED(1612, "角色编码已存在"),
    /**
     * 1613,角色ID不能为空
     */
    ROLE_ID_IS_NULL(1613, "角色ID不能为空"),

    /**
     * 1621,权限不存在
     */
    PERMISSION_NOT_FOUND(1621, "权限不存在"),
    /**
     * 1622,权限编码已存在
     */
    PERMISSION_CODE_EXISTED(1622, "权限编码已存在"),

    /**
     * 2001,登录失败，请重试
     */
    LOGIN_FAILED(2001, "登录失败，请重试"),

    /**
     * 2002,账号被禁用
     */
    LOGIN_USER_DISABLED(2002, "账号被禁用"),

    /**
     * 2003,手机号不存在
     */
    LOGIN_PHONE_NULL(2003, "手机号不存在"),

    /**
     * 2101,账号或密码错误
     */
    LOGIN_ACCOUNT_OR_PASSWORD_ERROR(2101, "登录名或密码错误"),

    /**
     * 2102,验证码错误
     */
    LOGIN_VERIFICATION_CODE_ERROR(2102, "验证码错误"),


    /**
     * 2201,手机号已经存在
     */
    REG_PHONE_DUPLICATION(2201, "手机号已经存在"),



    /**
     * 9999,服务器异常
     */
    SYS_EXP(9999, "服务器异常");


    private final Integer code;
    private final String msg;


    BusCode(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    /**
     * 方法描述: 枚举转换
     *
     * @param code code
     * @return BusCode BusCode
     */
    public static BusCode parseOf(int code) {
        for (BusCode item : values()) {
            if (item.getCode() == code) {
                return item;
            }
        }
        return null;
    }

    public static BusCode parseOf(String key) {
        return BusCode.parseOf(Integer.parseInt(key));
    }

    @Override
    public Integer getCode() {
        return this.code;
    }

    @Override
    public String getMsg() {
        return this.msg;
    }

    @Override
    public String toString() {
        return "BusCode{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                '}';
    }

}
