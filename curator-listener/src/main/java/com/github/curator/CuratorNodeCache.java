package com.github.curator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * 这个节点监听操作，不会监听删除事件，而且这种监听操作用到的场景很少
 * 
 * @author wangzhifeng
 * @date 2018年9月10日 下午3:07:18
 */
public class CuratorNodeCache {

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

        //4、建立一个Cache缓存
        final NodeCache cache = new NodeCache(cf, "/curator/nodecache", false);
        cache.start();

        ExecutorService pool = Executors.newCachedThreadPool();

        //创建监听器
        cache.getListenable().addListener(new NodeCacheListener() {

            public void nodeChanged() throws Exception {
                ChildData data = cache.getCurrentData();
                System.out.println("修改路径\t" + data.getPath());
                System.out.println("数据类容\t" + new String(data.getData()));
                System.out.println("状态\t" + data.getStat());
                System.out.println("-------------------------");
            }
        }, pool);

        //创建节点
        Thread.sleep(1000);
        cf.create().withMode(CreateMode.PERSISTENT).forPath("/curator/nodecache", "nodecache  test".getBytes());

        //数据修改
        Thread.sleep(1000);
        cf.setData().forPath("/curator/nodecache", "update".getBytes());

        //获取节点数据
        Thread.sleep(1000);
        byte[] data = cf.getData().forPath("/curator/nodecache");
        System.out.println(new String(data));

        //删除节点
        cf.delete().deletingChildrenIfNeeded().forPath("/curator/nodecache");
    }
}
//打印结果
//修改路径    /curator/nodecache
//数据类容    nodecache  test
//状态  47635846,47635846,1536563434629,1536563434629,0,0,0,0,15,0,47635846
//
//-------------------------
//修改路径    /curator/nodecache
//数据类容    update
//状态  47635846,47635855,1536563434629,1536563435653,1,0,0,0,6,0,47635846
//
//-------------------------
//update
