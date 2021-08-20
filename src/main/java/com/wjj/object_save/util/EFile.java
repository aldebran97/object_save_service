package com.wjj.object_save.util;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

/**
 * 扩展的文件类
 *
 * @author aldebran
 * @since 2021-08-12
 */
public class EFile extends File {

    public EFile(String pathname) {
        super(pathname);
    }

    public EFile(String parent, String child) {
        super(parent, child);
    }

    public EFile(File parent, String child) {
        super(parent, child);
    }

    public EFile(URI uri) {
        super(uri);
    }

    public EFile(File f) {
        super(f.getAbsolutePath());
    }

    /**
     * 创建文件时，判断父目录存在性
     *
     * @return 创建成功与否
     * @throws IOException
     */
    @Override
    public boolean createNewFile() throws IOException {
        if (isFile()) {
            return true;
        } else if (isDirectory()) {
            return false;
        }
        File p = this.getParentFile();
        if (p != null && !p.mkdirs()) {
            return false;
        }
        return super.createNewFile();
    }

    private boolean _del() {
        return super.delete();
    }

    private static boolean del(File file) {
        if (file instanceof EFile) {
            EFile eFile = (EFile) file;
            return eFile._del();
        }
        return file.delete();
    }

    /**
     * 删除文件或目录，目录可非空
     *
     * @return 操作是否成功
     */
    @Override
    public boolean delete() {
        Stack<File> stack = new Stack<>();
        stack.push(this);
        while (!stack.isEmpty()) {
            File f = stack.pop();
            if (f.isFile()) {
                if (!EFile.del(f)) {
                    return false;
                }
            } else {
                File[] subs = f.listFiles();
                if (subs == null) {
                    return false;
                }
                if (subs.length == 0) {
                    EFile.del(f);
                } else {
                    stack.push(f);
                    for (File sub : subs) {
                        stack.push(sub);
                    }
                }
            }
        }
        return true;
    }

    /**
     * 获取一个文件的字节内容
     *
     * @return 字节数组
     * @throws IOException
     */
    public byte[] getBytes() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(this);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int data;
        while ((data = fileInputStream.read()) != -1) {
            byteArrayOutputStream.write(data);
        }
        fileInputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 以字符串形式获取文件的内容
     *
     * @param charset 字符集
     * @return 字符串
     * @throws IOException
     */
    public String getString(Charset charset) throws IOException {
        return new String(getBytes(), charset);
    }

    public String getString() throws IOException {
        return getString(StandardCharsets.UTF_8);
    }

    /**
     * 向文件中输出字节数组
     *
     * @param bytes  字节数组
     * @param append 是否尾加
     * @throws IOException
     */
    public void writeBytes(byte[] bytes, boolean append) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(this, append);
        for (byte b : bytes) {
            fileOutputStream.write(b);
        }
        fileOutputStream.close();
    }

    /**
     * 向文件中输出字符串
     *
     * @param str     字符串
     * @param append  是否尾加
     * @param charset 字符集
     * @throws IOException
     */
    public void writeString(String str, boolean append, Charset charset) throws IOException {
        writeBytes(str.getBytes(charset), append);
    }

    public void writeString(String str, boolean append) throws IOException {
        writeString(str, append, StandardCharsets.UTF_8);
    }

}
