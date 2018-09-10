package com.github.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @author wangzhifeng
 * @date 2018年9月10日 下午3:12:02 PathChildrenCache.StartMode • BUILD_INITIAL_CACHE
 *       //同步初始化客户端的cache，及创建cache后，就从服务器端拉入对应的数据(这个是NodeCache使用的方式) • NORMAL
 *       //异步初始化cache •POST_INITIALIZED_EVENT
 *       //异步初始化，初始化完成触发事件PathChildrenCacheEvent.Type.INITIALIZED (这个方式是
 *       PathChildrenCacheListener 使用的)
 */
public class CuratorPathChildrenCache {

    private static final String CONNECT_PATH     = "172.16.20.171:2181";
    // Session 超时时间
    private static final int    SESSION_TIME_OUT = 60000;

    //连接超时
    private static final int    CONNECT_TIME_OUT = 5000;

    public static void main(String[] args) throws Exception {

        //1、尝试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 10);

        //2、创建CuratorFramework 工厂类
        CuratorFramework cf = CuratorFrameworkFactory.builder().connectString(CONNECT_PATH)
                .connectionTimeoutMs(SESSION_TIME_OUT).sessionTimeoutMs(CONNECT_TIME_OUT).retryPolicy(retryPolicy)
                .build();

        //3、启动连接
        cf.start();

        //4、建立一个Cache缓存 ,第三个参数是 dataIsCompressed 是否进行数据压缩  ,需要配置为true
        //如果第三个参数设置为 false,则不接受节点变更后的数据
        final PathChildrenCache cache = new PathChildrenCache(cf, "/curator", true);

        //5、设定监听的模式 ,异步初始化，初始化完成触发事件 PathChildrenCacheEvent.Type.INITIALIZED
        cache.start(StartMode.POST_INITIALIZED_EVENT);

        //创建监听器
        cache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework cf, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();
                System.out.println("路径\t" + data.getPath());
                System.out.println("更改数据\t" + new String(data.getData()));
                System.out.println("节点状态\t" + data.getStat());
                System.out.println(event.getType());
                switch (event.getType()) {
                    //初始化会触发这个事件
                    case INITIALIZED:
                        System.out.println("子类缓存系统初始化完成");
                        break;
                    case CHILD_ADDED:
                        System.out.println("添加子节点");
                        break;
                    case CHILD_UPDATED:

                        System.out.println("更新子节点");
                        break;
                    case CHILD_REMOVED:
                        System.out.println("删除子节点");
                        break;
                    default:
                        break;
                }

                System.out.println("----------------------------------");
            }
        });

        //判断节点是否存在，然后删除掉
        Stat stat = cf.checkExists().forPath("/curator/nodecache2");
        if (stat != null) {
            Thread.sleep(1000);
            cf.delete().deletingChildrenIfNeeded().forPath("/curator/nodecache2");
        }

        //创建节点
        Thread.sleep(1000);
        cf.create().withMode(CreateMode.PERSISTENT).forPath("/curator/nodecache2", "nodecache  test".getBytes());

        //数据修改
        Thread.sleep(1000);
        cf.setData().forPath("/curator/nodecache2", "update".getBytes());

        //获取节点数据
        Thread.sleep(1000);
        byte[] data = cf.getData().forPath("/curator/nodecache2");
        System.out.println(new String(data));

        //删除节点
        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath("/curator/nodecache2");
    }
}
//打印
//路径  /curator/nodecache2
//更改数据    nodecache  test
//节点状态    47639482,47639482,1536564231008,1536564231008,0,0,0,0,15,0,47639482
//
//CHILD_ADDED
//添加子节点
//----------------------------------
//路径  /curator/nodecache2
//更改数据    update
//节点状态    47639482,47639487,1536564231008,1536564232020,1,0,0,0,6,0,47639482
//
//CHILD_UPDATED
//更新子节点
//----------------------------------
//update
//路径  /curator/nodecache2
//更改数据    update
//节点状态    47639482,47639487,1536564231008,1536564232020,1,0,0,0,6,0,47639482
//
//CHILD_REMOVED
//删除子节点
//----------------------------------
