package com.aifurion.display.common;

/**
 * 公共异常抽象类，多个服务时每个服务单独实现此接口
 *
 * @author zzy
 */
public interface IBusCode {

    /**
     * 获取错误码
     *
     * @return 错误码
     */
    Integer getCode();

    /**
     * 获取错误信息
     *
     * @return 错误信息
     */
    String getMsg();

}
