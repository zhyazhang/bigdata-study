package com.aifurion;

import com.aifurion.utils.LogUploader;

import java.io.*;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/5 14:32
 */
public class LogMockerApp {

    public static void main(String[] args) {

        //文件地址
        String filePath = "E:/data/ImoocLog/access.20161111.log";

        try (InputStream inputStream = new FileInputStream(filePath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            //一定要增大nginx的worker_connections连接数，否则写入一段时间后会出错
            while (reader.ready()) {
                String line = reader.readLine();
                LogUploader.sendLogStream(line);
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException();
        }


    }


}
