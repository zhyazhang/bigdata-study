package com.aifurion.display.beans;

/**
 * @author ：zzy
 * @description：TODO 省浏览量实体类
 * @date ：2021/12/7 9:28
 */
public class ProvinceView {

    private String province;

    private Integer count;

    public ProvinceView() {
    }

    public ProvinceView(String province, Integer count) {
        this.province = province;
        this.count = count;
    }


    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ProvinceView{" +
                "province='" + province + '\'' +
                ", count=" + count +
                '}';
    }
}
