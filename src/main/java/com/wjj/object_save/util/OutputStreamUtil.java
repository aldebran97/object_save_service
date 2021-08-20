package com.wjj.object_save.util;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 输出流工具
 *
 * @author aldebran
 * @since 2021-08-12
 */
public class OutputStreamUtil {

    // 输入流-->输出流
    public static void write(InputStream inputStream, OutputStream outputStream) throws IOException {
        int data;
        while ((data = inputStream.read()) != -1) {
            outputStream.write(data);
        }
        inputStream.close();
        outputStream.close();
    }

    // 字节数组-->输出流
    public static void writeBytes(byte[] bytes, OutputStream outputStream) throws IOException {
        write(new ByteArrayInputStream(bytes), outputStream);
    }

    // 字符串-->输出流
    public static void writeString(String str, OutputStream outputStream, Charset charset) throws IOException {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, charset);
        outputStreamWriter.write(str);
        outputStreamWriter.close();
    }

    public static void writeString(String str, OutputStream outputStream) throws IOException {
        writeString(str, outputStream, StandardCharsets.UTF_8);
    }
}
