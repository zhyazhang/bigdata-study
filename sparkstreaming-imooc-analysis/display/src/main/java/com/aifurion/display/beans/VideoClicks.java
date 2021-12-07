package com.aifurion.display.beans;

/**
 * @author ：zzy
 * @description：TODO 视频点击量实体类
 * @date ：2021/12/7 14:06
 */
public class VideoClicks {


    private String videoId;

    private Integer clickCount;

    public VideoClicks() {
    }

    public VideoClicks(String videoId, Integer clickCount) {
        this.videoId = videoId;
        this.clickCount = clickCount;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }


    @Override
    public String toString() {
        return "VideoClicks{" +
                "videoId='" + videoId + '\'' +
                ", clickCount=" + clickCount +
                '}';
    }
}
