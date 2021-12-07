package com.aifurion.display.service.impl;

import com.aifurion.display.beans.ProvinceView;
import com.aifurion.display.service.LoadDataService;
import com.aifurion.display.utils.JedisPoolUtil;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:56
 */


@Service
public class LoadDataServiceImpl implements LoadDataService {


    /**
     * 获得每个省的浏览次数
     *
     * @return 没省浏览次数
     */
    @Override
    public List<ProvinceView> getProvincePV() {

        Jedis jedis = null;

        List<ProvinceView> list = new ArrayList<>();

        try {
            jedis = JedisPoolUtil.getConnection();

            Map<String, String> popCount = jedis.hgetAll("cityPopCount");

            for (Map.Entry<String, String> entry : popCount.entrySet()) {

                //去除全球数据
                if (!"全球".equals(entry.getKey().trim())) {
                    ProvinceView provinceView = new ProvinceView(entry.getKey(),
                            Integer.parseInt(entry.getValue()));
                    list.add(provinceView);

                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) jedis.close();
        }

        return list;
    }
}
