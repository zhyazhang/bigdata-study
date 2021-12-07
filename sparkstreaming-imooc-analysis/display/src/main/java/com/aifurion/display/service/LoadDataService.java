package com.aifurion.display.service;

import com.aifurion.display.beans.ProvinceView;
import com.aifurion.display.beans.VideoClicks;

import java.util.List;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:55
 */
public interface LoadDataService {


    /**
     * 获得每个省的浏览次数
     * @return 每省浏览次数
     */
    List<ProvinceView> getProvincePV();


    /**
     * 获得视频浏览量top 7
     * @return top 7视频id和浏览次数
     */
    List<VideoClicks> getVideoClicks();

}
