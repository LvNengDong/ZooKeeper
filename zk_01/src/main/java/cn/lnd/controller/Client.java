package cn.lnd.controller;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * @Author lnd
 * @Description
 * @Date 2022/9/19 11:36
 */
@RequestMapping("/client")
@RestController()
public class Client {

    String connectString = "121.4.221.191:2181";
    Integer sessionTimeout = Integer.MAX_VALUE; //会话超时时间
    Watcher watcher = (event) -> System.out.println(event); //第一次连接时用的监听器，不能为null
    ZooKeeper zkClient = null;
    String basePath = "/servers";

    /**
     * 注册到ZK
     */
    @GetMapping("/register")
    public String register() {
        try {
            zkClient = new ZooKeeper(connectString, sessionTimeout, watcher);
            System.out.println("Client已成功连接到ZK");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Client已成功连接到ZK";
    }


    /**
     * 客户端持续监听注册到ZK上的服务器的上下线信息
     */
    @GetMapping("/monitor")
    public String monitor(){
        try {
            List<String> children = zkClient.getChildren(basePath, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getPath() + "路径发生了" + watchedEvent.getType() + "事件");
                    //递归，持续监听
                    monitor();
                }
            });
            //获取每个服务器节点中保存的信息
            for (String child : children) {
                System.out.println(child);
            }
            return children.toString();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
