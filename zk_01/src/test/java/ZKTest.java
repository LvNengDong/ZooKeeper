import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

/**
 * @Author lnd
 * @Description
 * @Date 2022/9/16 1:04
 */
public class ZKTest {
    // 格式：`IP地址:端口号`，如果ZK是一个集群，多个地址之间用逗号隔开
    String connectString = "121.4.221.191:2181, hadoop102:2181,";
    //String connectString = "hadoop101:2181, hadoop102:2181,";

    // session的超时时间（单位：ms）
    int sessionTimeout = 1000000;
    // 观察者实例，一旦watcher观察的path发生了变更，服务端就会通知客户端，客户端收到通知后就会自动调用process()方法
    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {

        }
    };

    ZooKeeper zkClient = null;

    // 创建ZK客户端对象
    @BeforeEach
    @Test
    public void init() throws IOException, InterruptedException {
        // 创建一个ZK客户端对象，创建 zkClient 需要依赖 connectString, sessionTimeout, watcher
        zkClient = new ZooKeeper(connectString, sessionTimeout, watcher);
        System.out.println(zkClient);
        Thread.sleep(3000);	// 主动让ZKClient睡眠一段时间，保证TCP连接可以成功建立
        System.out.println("init");
    }

    // 使用完毕后释放 zkClient
    @AfterEach
    @Test
    public void close() throws InterruptedException {
        if (zkClient != null){
            zkClient.close();
        }
        System.out.println("end");
    }
}
