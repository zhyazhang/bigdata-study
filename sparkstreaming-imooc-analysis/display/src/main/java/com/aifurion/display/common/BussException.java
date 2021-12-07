package com.aifurion.display.common;

/**
 * 全局业务异常
 *
 * @author zzy
 */
public class BussException extends RuntimeException {
    private static final long serialVersionUID = 4701809334340182916L;

    private final static boolean WRITABLE_STACK_TRACE = true;

    private IBusCode code;
    private String msg;

    public BussException(IBusCode code) {
        super(code.getMsg(), null, true, WRITABLE_STACK_TRACE);
        this.setCode(code);
        this.setMsg(code.getMsg());
    }

    public BussException(String e) {
        super(e, null, true, WRITABLE_STACK_TRACE);
        this.setCode(BusCode.SYS_EXP);
        this.setMsg(e);
    }

    public BussException(String msg, Throwable e) {
        super(msg, e, true, WRITABLE_STACK_TRACE);
        this.setCode(BusCode.SYS_EXP);
        this.setMsg(msg);
    }

    public BussException(BusCode code, String msg) {
        super(msg, null, true, WRITABLE_STACK_TRACE);
        this.setCode(code);
        this.setMsg(msg);
    }

    public IBusCode getCode() {
        return code;
    }

    public void setCode(IBusCode code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
