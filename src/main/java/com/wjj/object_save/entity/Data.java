package com.wjj.object_save.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 数据实体定义
 *
 * @author aldebran
 * @since 2021-08-19
 */
public class Data {

    public String id; // 全局唯一

    public long md5; // 不一定唯一，但查找速度快

    public long qId; // 仅用于分页查询优化，数据迁移时忽略

    public byte[] content; // 文件内容

    public Data() {
    }


    public Data(ResultSet resultSet) throws SQLException {
        id = resultSet.getString(1);
        md5 = resultSet.getLong(2);
        qId = resultSet.getLong(3);
        content = resultSet.getBytes(4);
    }

    public void setIdAndMd5(String bucketName, String fileName) {
        id = bucketName + "@" + fileName;
        md5 = Integer.toUnsignedLong(id.hashCode());
    }

    public String getBucketName() {
        int index = id.indexOf("@");
        return id.substring(0, index);
    }

    public String getFileName() {
        int index = id.indexOf("@");
        return id.substring(index + 1);
    }

    @Override
    public String toString() {
        return "Data{" +
                "md5=" + md5 +
                ", id='" + id + '\'' +
                ", content=" + content +
                '}';
    }


}
