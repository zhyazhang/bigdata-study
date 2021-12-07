package com.aifurion.display.controller;

import com.aifurion.display.beans.ProvinceView;
import com.aifurion.display.common.ApiResponse;
import com.aifurion.display.service.LoadDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:54
 */


@RestController
public class LoadDataController {

    @Autowired
    private LoadDataService loadDataService;


    @GetMapping("/getProvince")
    public ApiResponse<List<ProvinceView>> getProvincePV() {

        return ApiResponse.success(loadDataService.getProvincePV());
    }






}
