package com.aifurion.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/5 14:40
 */
public class LogUploader {

    private static int iter = 0;


    public static void sendLogStream(String log) {


        try {

            //nginx url地址
            URL url = new URL("http://hadoop102:8083/savelog");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime", System.currentTimeMillis() + "");
            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为TEXT
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");

            iter++;

            System.out.println(iter);

            //输出流
            OutputStream out = conn.getOutputStream();

            out.write((log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
