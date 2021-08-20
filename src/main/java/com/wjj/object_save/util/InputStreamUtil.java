package com.wjj.object_save.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 输入流工具
 *
 * @author aldebran
 * @since 2021-08-12
 */
public class InputStreamUtil {

    // 输入流转字节数组
    public static byte[] toBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int data;
        while ((data = inputStream.read()) != -1) {
            byteArrayOutputStream.write(data);
        }
        inputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    // 输入流转字符串
    public static String toStr(InputStream inputStream, Charset charset) throws IOException {
        return new String(toBytes(inputStream), charset);
    }

    public static String toStr(InputStream inputStream) throws IOException {
        return toStr(inputStream, StandardCharsets.UTF_8);
    }
}
