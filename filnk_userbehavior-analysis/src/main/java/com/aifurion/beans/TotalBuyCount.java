package com.aifurion.beans;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 16:43
 */
public class TotalBuyCount {
    private String url;

    private Long windowEnd;

    private Long count;


    public TotalBuyCount() {
    }

    public TotalBuyCount(String url,Long windowEnd, Long count ) {
        this.windowEnd = windowEnd;
        this.count = count;
        this.url = url;
    }

    @Override
    public String toString() {
        return "TotalBuyCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                ", url='" + url + '\'' +
                '}';
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
