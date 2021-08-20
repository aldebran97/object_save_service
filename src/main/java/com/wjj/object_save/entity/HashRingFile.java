package com.wjj.object_save.entity;

import com.wjj.object_save.util.EFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;

/**
 * 使用文件存储方式的哈希环
 * 看实际情况使用，有时因为安全性不能采用文件存储，例如加密解密，此时需要对HashRing再次封装
 *
 * @author aldebran
 * @since 2021-08-18
 */
public class HashRingFile {

    public EFile file;

    private HashRing hashRing;

    public HashRingFile(String fileName) throws IOException {
        file = new EFile(fileName);
        if (!file.createNewFile()) {
            throw new IOException("fail to create file: " + fileName);
        }
        hashRing = new HashRing(new FileInputStream(file), StandardCharsets.UTF_8);
    }

    public void save() throws IOException {
        hashRing.save(new FileOutputStream(file));
    }

    public void init(int number, List<ConnectionInfo> connectionInfos) {
        hashRing.init(number, connectionInfos);
    }

    public int addNode(ConnectionInfo connectionInfo) throws SQLException {
        return hashRing.addNode(connectionInfo);
    }

    public void saveData(Data data) throws SQLException {
        hashRing.saveData(data);
    }

    public Data getData(Data data) throws SQLException {
        return hashRing.getData(data);
    }

    public boolean deleteData(Data data) throws SQLException {
        return hashRing.deleteData(data);
    }

    // 指定移除节点
    public void remove(int index) throws SQLException {
        hashRing.remove(index);
    }

    // 自动移除节点
    public int remove() throws SQLException {
        return hashRing.remove();
    }

    public void destroy() throws SQLException {
        hashRing.destroy();
    }
}
