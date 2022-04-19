package com.ct.producer.io;

import com.ct.common.bean.DataOut;

import java.io.*;

public class LocalFIleDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFIleDataOut(){

    }
    public LocalFIleDataOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) throws IOException {
        write(data.toString());
    }

    /**
     * 将数据字符串写入到文件中
     * @param data
     * @throws IOException
     */
    public void write(String data) throws IOException {
        writer.println(data);
        writer.flush();
    }

    public void close() throws IOException {
        if (writer != null){
            writer.close();
        }

    }
}
