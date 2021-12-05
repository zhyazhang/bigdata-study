package com.aifurion;

import java.io.*;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/12/5 14:32
 */
public class LogMocker {

    public static void main(String[] args) {

        //文件地址
        String filePath = "E:/data/ImoocLog/access.20161111.log";

        try (InputStream inputStream = new FileInputStream(filePath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

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
