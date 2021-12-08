package com.aifurion.display.service.impl;

import com.aifurion.display.beans.ProvinceView;
import com.aifurion.display.beans.ValueView;
import com.aifurion.display.beans.VideoClicks;
import com.aifurion.display.service.LoadDataService;
import com.aifurion.display.utils.JedisPoolUtil;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/7 8:56
 */


@Service
public class LoadDataServiceImpl implements LoadDataService {


    @Override
    public ValueView getViewCount() {

        ValueView value = new ValueView();
        try (Jedis jedis = JedisPoolUtil.getConnection()) {
            value.setTotalclick(Integer.parseInt(jedis.hget("value", "totalclick")));
            value.setPerclick(Integer.parseInt(jedis.hget("value", "perclick")));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return value;
    }


    /**
     * 获得浏览量最大top 6的省
     *
     * @return top 6的省和浏览数
     */
    @Override
    public List<ProvinceView> getProvincePVTopN() {


        List<ProvinceView> list = new ArrayList<>();

        try (Jedis jedis = JedisPoolUtil.getConnection()) {

            Map<String, String> map = jedis.hgetAll("cityPopCount");

            //转换Map的数据类型Map<String,String> ==> Map<String,Integer>
            Map<String, Integer> integerMap =
                    map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            m -> Integer.parseInt(m.getValue())));

            //求得浏览量之和
            Integer sum = 0;
            for (Integer v : integerMap.values()) {
                sum += v;
            }

            //排序
            ArrayList<Map.Entry<String, Integer>> entryArrayList =
                    new ArrayList<>(integerMap.entrySet());

            entryArrayList.sort(Map.Entry.comparingByValue());

            int size = entryArrayList.size();
            int top6Count = 0;

            for (int i = 1; i <= 6; i++) {
                Map.Entry<String, Integer> entry = entryArrayList.get(size - i);

                top6Count += entry.getValue();

                list.add(new ProvinceView(entry.getKey(), entry.getValue()));
            }

            //添加除top-6之外的其它省的数据
            list.add(new ProvinceView("其它", sum - top6Count));

        } catch (Exception e) {
            e.printStackTrace();

        }

        return list;
    }


    /**
     * 获得视频浏览量top 7
     *
     * @return top 7视频id和浏览次数
     */
    @Override
    public List<VideoClicks> getVideoClicks() {

        List<VideoClicks> list = new ArrayList<>();
        try (Jedis jedis = JedisPoolUtil.getConnection()) {

            Map<String, String> map = jedis.hgetAll("videoPopCount");
            ArrayList<Map.Entry<String, String>> arrayList = new ArrayList<>(map.entrySet());
            arrayList.sort(Map.Entry.comparingByValue());

            int size = arrayList.size();

            for (int i = 1; i <= 7; i++) {
                Map.Entry<String, String> entry = arrayList.get(size - i);
                list.add(new VideoClicks(entry.getKey(), Integer.parseInt(entry.getValue())));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
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
            if (jedis != null) {
                jedis.close();
            }
        }

        return list;
    }


}
