package com.wjj.object_save;

import com.wjj.object_save.entity.ConnectionInfo;
import com.wjj.object_save.entity.Data;
import com.wjj.object_save.entity.HashRingFile;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

//@SpringBootApplication
public class ObjectSaveApplication {

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        System.out.println("成功加载jdbc驱动");
    }

    public static void main(String[] args) throws Exception {

        /*
         * 开始之前，确保每个节点的数据库中都执行了sql/create.sql脚本，数据库名随意，接下来可以配置
         * 创建哈希环
         */
        HashRingFile hashRingFile = new HashRingFile("/home/HashRing.json");

        /*
         * 如果第一次使用，没有配置，可以调用init()自动生成配置，假如只有两个节点
         */
        ConnectionInfo c1 = new ConnectionInfo(); // mysql connection info
        c1.port = 3306;
        c1.host = "your host";
        c1.dbName = "your bd name";
        c1.password = "your password";
        c1.userName = "your username";

        ConnectionInfo c2 = new ConnectionInfo();
        c2.port = 3306;
        c2.host = "your host";
        c2.dbName = "your bd name";
        c2.password = "your password";
        c2.userName = "your username";

        // 初始化哈希环，自动生成n个节点
        hashRingFile.init(2, Arrays.asList(c1, c2));
        hashRingFile.save(); // 别忘了持久化

        // 如果后续需要横向添加节点，调用addNode()，自动添加到合适的位置上
//        hashRingFile.addNode(connectionInfo);
//        hashRingFile.save(); // 别忘了持久化


        // 如果要移除节点，调用remove，自动寻找最适合移除的节点（负担小）
//        hashRingFile.remove();
//        hashRingFile.save(); // 别忘了持久化

        // 也可以移除，指定节点，如果其负载大的话
//        hashRingFile.remove(1);
//        hashRingFile.save();

        // !!! 如果配置已经确定，并且不再扩容减容，构造后不需要再调用init(), addNode()和remove()方法了，可以直接存取数据

        // 尝试存储数据（或修改数据）
        Data testData = new Data();
        testData.setIdAndMd5("your bucketName", "your fileName");
        testData.content = "your data".getBytes(StandardCharsets.UTF_8);
        hashRingFile.saveData(testData);

        // 尝试查询数据
        Data qData = new Data();
        qData.setIdAndMd5("your bucketName", "your fileName");
        qData = hashRingFile.getData(qData); // qData为null，说明数据不存在，否则取qData.content就能得到内容
        if (qData != null) {
            System.out.println(new String(qData.content)); // 如果不是字符串，不用new String()... 这里仅仅示例
            System.out.println("bucket name: " + qData.getBucketName());
            System.out.println("file name: " + qData.getFileName());
        } else {
            System.out.println("数据不存在！");
        }

        // 尝试删除数据
        Data deleteData = new Data();
        deleteData.setIdAndMd5("your bucketName", "your fileName");
        if (!hashRingFile.deleteData(deleteData)) { // 返回true说明数据存在并删除成功，false说明数据不存在，有时候也达到了目的!
            System.out.println("数据删除失败，数据不存在！");
        }

        // 如果不再使用对象存储服务，可以调用destroy()，只是内存级别的关闭，持久化文件依旧保留，再次构造还可以正常使用
//        hashRingFile.destroy();

//        SpringApplication.run(ObjectSaveApplication.class, args);
    }

}
