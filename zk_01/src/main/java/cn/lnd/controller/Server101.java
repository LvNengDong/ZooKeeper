package cn.lnd.controller;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author lnd
 * @Description 服务器节点
 * @Date 2022/9/19 11:36
 */
@RequestMapping("/server101")
@RestController
public class Server101 {
    /*
        1、连接到ZK
        2、注意：服务器节点一定不能是永久节点，因为对于服务器来说，下线后要求ZK上对应的节点数据就要
        被删除，而永久节点一旦被创建后就会持久化，即使下线了也不会在ZK中失去这个节点的信息
        3、上线：向ZK添加服务器的数据
        4、下线：从ZK删除服务器的信息
    */
    String connectString = "121.4.221.191:2181";
    Integer sessionTimeout = Integer.MAX_VALUE; //会话超时时间
    Watcher watcher = (event) -> System.out.println(event); //第一次连接时用的监听器，不能为null

    ZooKeeper zkClient = null;
    String basePath = "/servers";
    //String serverName = "/Hadoop101";
    String serverData = "This is a info";

    /**
     * 连接到ZK
     */
    @GetMapping("/register")
    public String register() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, watcher);
        System.out.println("ZooKeeper init success");
        return "ZooKeeper init success";
    }

    /**
     * 上线
     */
    @GetMapping("/online/{serverName}")
    public String online(@PathVariable String serverName){
        try {
            //将服务器自身的信心注册到ZK上。注意一定要创建成临时节点！！！
            zkClient.create(basePath + "/" + serverName, serverData.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
            /*注意：能创建 /servers/hadoop101 的前提一定得有 /servers 目录，如果没有，则需要提前创建 */
            System.out.println(serverName + "已上线");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return serverName + "已上线";
    }

    /**
     * 下线
     */
    @GetMapping("/offline/{serverName}")
    public String offline(@PathVariable String serverName){
        Stat stat = null;
        try {
            //获取节点的版本号
            //stat = zkClient.exists(basePath + "/" + serverName, false);
            //int version = stat.getVersion();
            //删除节点（下线节点）
            zkClient.delete(basePath + "/" + serverName, -1); //-1代表任何版本
            //zkClient.delete(basePath + serverName, version);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return serverName + "已下线";
    }
}
