package com.aifurion.display.beans;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/8 15:39
 */
public class ValueView {


    private int totalclick;

    private int perclick;


    public ValueView() {
    }

    public ValueView(int totalclick, int perclick) {
        this.totalclick = totalclick;
        this.perclick = perclick;
    }


    @Override
    public String toString() {
        return "ValueView{" +
                "totalclick=" + totalclick +
                ", perclick=" + perclick +
                '}';
    }

    public int getTotalclick() {
        return totalclick;
    }

    public void setTotalclick(int totalclick) {
        this.totalclick = totalclick;
    }

    public int getPerclick() {
        return perclick;
    }

    public void setPerclick(int perclick) {
        this.perclick = perclick;
    }
}
