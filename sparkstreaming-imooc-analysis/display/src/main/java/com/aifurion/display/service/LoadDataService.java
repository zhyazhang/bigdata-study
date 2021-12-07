package com.aifurion.display.service;

import com.aifurion.display.beans.ProvinceView;

import java.util.List;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:55
 */
public interface LoadDataService {


    /**
     * 获得每个省的浏览次数
     * @return 没省浏览次数
     */
    List<ProvinceView> getProvincePV();


}
