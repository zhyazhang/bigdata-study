package com.aifurion.display.service.impl;

import com.aifurion.display.beans.ProvinceView;
import com.aifurion.display.beans.VideoClicks;
import com.aifurion.display.service.LoadDataService;
import com.aifurion.display.utils.JedisPoolUtil;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:56
 */


@Service
public class LoadDataServiceImpl implements LoadDataService {


    /**
     * 获得视频浏览量top 7
     *
     * @return top 7视频id和浏览次数
     */
    @Override
    public List<VideoClicks> getVideoClicks() {

        long start = System.currentTimeMillis();

        List<VideoClicks> list = new ArrayList<>();
        try (Jedis jedis = JedisPoolUtil.getConnection()) {

            Map<String, String> map = jedis.hgetAll("videoPopCount");

            ArrayList<Map.Entry<String, String>> arrayList = new ArrayList<>(map.entrySet());

            Collections.sort(arrayList, Map.Entry.comparingByValue());


            int size = arrayList.size();


            for (int i = 1; i <= 7; i++) {
                Map.Entry<String, String> entry = arrayList.get(size - i);

                list.add(new VideoClicks(entry.getKey(), Integer.parseInt(entry.getValue())));

            }


        } catch (Exception e) {
            e.printStackTrace();

        }

        long end = System.currentTimeMillis();

        System.out.println(end - start);


        return list;
    }


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
