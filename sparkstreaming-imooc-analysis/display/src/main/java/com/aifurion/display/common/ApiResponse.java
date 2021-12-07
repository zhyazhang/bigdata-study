package com.aifurion.display.common;

/**
 * 返回消息封装
 *
 * @author zzy
 */

public class ApiResponse<T> {

    private Integer code;

    private String message;

    private T data;


    public ApiResponse() {
    }

    public ApiResponse(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }


    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }


    @Override
    public String toString() {
        return "ApiResponse{" +
                "code=" + code +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }

    public static <T> ApiResponse<T> success() {
        return success(null);
    }

    public static <T> ApiResponse<T> success(T data) {

        return new ApiResponse<>(BusCode.SUCCESS.getCode(), BusCode.SUCCESS.getMsg(), data);

    }

    public static <T> ApiResponse<T> error() {

        return new ApiResponse<>(BusCode.SYS_EXP.getCode(), BusCode.SYS_EXP.getMsg(), null);

    }

    public static <T> ApiResponse<T> error(String message) {

        return new ApiResponse<>(BusCode.SYS_EXP.getCode(), message, null);

    }

    public static <T> ApiResponse<T> error(IBusCode code, String message) {

        return new ApiResponse<>(code.getCode(), message, null);

    }

    public static <T> ApiResponse<T> result(Integer code, String message) {

        return new ApiResponse<>(code, message, null);

    }

    public static <T> ApiResponse<T> result(IBusCode code) {

        return new ApiResponse<>(code.getCode(), code.getMsg(), null);

    }

    public static <T> ApiResponse<T> result(IBusCode code, String msg) {

        return new ApiResponse<>(code.getCode(), msg, null);

    }

    public static <T> ApiResponse<T> result(IBusCode code, T t) {


        return new ApiResponse<>(code.getCode(), code.getMsg(), t);

    }

}
