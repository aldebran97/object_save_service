package com.wjj.object_save.mapper;

import com.mysql.cj.jdbc.JdbcConnection;
import com.wjj.object_save.entity.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Data存取器
 *
 * @author aldebran
 * @since 2021-08-17
 */
public class DataMapper {

    public static final String tableName = "oss";

    private static final Logger log = LoggerFactory.getLogger(DataMapper.class);

    /**
     * 用于仅仅判断id存在性
     *
     * @param id             id
     * @param jdbcConnection 数据库连接
     * @return
     * @throws SQLException
     */
    public static boolean idExists(String id, JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        String sql = String.format("select count(*) from %s where id = '%s'",
                tableName, id);
        log.debug(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        int count = resultSet.getInt(1);
        resultSet.close();
        return count > 0;
    }

    /**
     * 用于判断MD5存在性和数量
     *
     * @param md5            MD5
     * @param jdbcConnection 数据库连接
     * @return
     * @throws SQLException
     */
    public static int md5Count(long md5, JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        String sql = String.format("select count(*) from %s where md5 = %s",
                tableName, md5);
        log.debug(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        int count = resultSet.getInt(1);
        resultSet.close();
        return count;
    }

    /**
     * 根据ID查询数据
     *
     * @param id             id
     * @param jdbcConnection 连接
     * @return
     * @throws SQLException
     */
    public static Data queryById(String id, JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        String sql = String.format("select id, md5, qId, content from %s where id = '%s'",
                tableName, id);
        log.debug(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        if (!resultSet.next()) {
            return null;
        } else {
            Data data = new Data(resultSet);
            resultSet.close();
            return data;
        }
    }

    /**
     * 根据qId查询信息，相比id，qId是唯一性、数值型索引
     *
     * @param qId            qId
     * @param jdbcConnection 连接
     * @return
     * @throws SQLException
     */
    public static Data queryByQId(long qId, JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        String sql = String.format("select id, md5, qId, content from %s where qId = %s",
                tableName, qId);
        log.debug(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        if (!resultSet.next()) {
            return null;
        } else {
            Data data = new Data(resultSet);
            resultSet.close();
            return data;
        }
    }

    /**
     * 通过MD5查询数据
     *
     * @param md5
     * @param jdbcConnection
     * @return
     * @throws SQLException
     */
    public static List<Data> queryByMd5(long md5, JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        String sql = String.format("select id, md5, qId, content from %s where md5 = %s",
                tableName, md5);
        log.debug(sql);
        ResultSet resultSet = statement.executeQuery(sql);
        List<Data> result = new ArrayList<>();
        while (resultSet.next()) {
            Data data = new Data(resultSet);
            result.add(data);
        }
        resultSet.close();
        return result;
    }

    // 保存数据
    public static void saveData(Data data, JdbcConnection jdbcConnection) throws SQLException {
        Data qData = queryById(data.id, jdbcConnection);
        String sql;
        if (qData == null) {
            sql = "insert into oss (id, md5, content) values (?, ?, ?)";
            log.debug(sql);
            PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql);
            preparedStatement.setString(1, data.id);
            preparedStatement.setLong(2, data.md5);
            preparedStatement.setBinaryStream(3, new ByteArrayInputStream(data.content));
            preparedStatement.executeUpdate();
        } else {
            sql = "update oss set content = ? where id = ?";
            PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql);
            preparedStatement.setBinaryStream(1, new ByteArrayInputStream(data.content));
            preparedStatement.setString(2, data.id);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * 查询所有数据，采用迭代器避免OOM，采用qId条件查询而不是传统分页提高速度
     *
     * @param jdbcConnection 连接
     * @param limitSize      分页大小
     * @return
     * @throws SQLException
     */
    public static Iterator<Data> selectAll(JdbcConnection jdbcConnection, Integer limitSize) throws SQLException {

        Iterator<Data> iterator = new Iterator<Data>() {

            int limit = limitSize == null ? 1000 : limitSize;
            long maxQId = -1;
            List<Data> dataList = new ArrayList<>();
            int index = 0;

            @Override
            public boolean hasNext() {
                if (index >= dataList.size()) {
                    Statement statement = null;
                    try {
                        statement = jdbcConnection.createStatement();
                        String sql = String.format(
                                "select id, md5, qId, content from oss where qId > %s order by qId limit %s",
                                maxQId, limit);
                        log.debug(sql);
                        ResultSet resultSet = statement.executeQuery(sql);
                        // 重置列表和索引
                        dataList.clear();
                        while (resultSet.next()) {
                            dataList.add(new Data(resultSet));
                        }
                        statement.close();
                        index = 0;
                        // 选择最大qId
                        for (Data data : dataList) {
                            if (data.qId > maxQId) {
                                maxQId = data.qId;
                            }
                        }
                    } catch (SQLException throwables) {
                        return false;
                    } finally {
                        if (statement != null) {
                            try {
                                statement.close();
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                            }
                        }
                    }
                }
                return index < dataList.size();
            }

            @Override
            public Data next() {
                return dataList.get(index++);
            }
        };

        return iterator;
    }

    /**
     * 根据ID删除文件
     *
     * @param jdbcConnection 连接
     * @param id             id
     * @throws SQLException
     */
    public static boolean deleteById(JdbcConnection jdbcConnection, String id) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        int result = statement.executeUpdate(String.format("delete from oss where id = '%s'", id));
        statement.close();
        return result == 1;
    }

    /**
     * 求数据的数量
     *
     * @param jdbcConnection 连接
     * @return
     * @throws SQLException
     */
    public static long count(JdbcConnection jdbcConnection) throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from oss");
        resultSet.next();
        long count = resultSet.getLong(1);
        statement.close();
        return count;
    }


}
