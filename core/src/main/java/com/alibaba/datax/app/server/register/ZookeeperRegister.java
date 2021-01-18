package com.alibaba.datax.app.server.register;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/14 10:39
 * @since 1.0.0
 */
public class ZookeeperRegister {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperRegister.class);
    private static final String NODE_NAME = "cluster/server";
    private static final String ROOT_PATH = "cluster";

    private ZookeeperRegister() {
    }

    private static final CuratorFramework CURATOR_FRAMEWORK;

    static {
        RetryPolicy backoffRetry = new ExponentialBackoffRetry(3000, 2);
        CURATOR_FRAMEWORK = CuratorFrameworkFactory.builder()
                .connectString(getZkAddress())
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(30000)
                .retryPolicy(backoffRetry)
                .namespace("datax")
                .build();
        CURATOR_FRAMEWORK.start();
    }

    public static void register(String data) {
        try {
            CURATOR_FRAMEWORK.create()
                    .creatingParentsIfNeeded()
                    // 临时有序节点
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath("/" + NODE_NAME, data.getBytes(StandardCharsets.UTF_8));
            LOG.info("Datax Server register, info: {}", data);
        } catch (Exception e) {
            LOG.error("register server error");
            throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, "DataX Server注册失败", e);
        }
    }

    public static boolean exist(String url) {
        Optional<RegisterDataxInfo> optional = list().stream()
                .filter(o -> o.getUrl().equals(url))
                .findFirst();
        return optional.isPresent();
    }

    public static List<RegisterDataxInfo> list() {
        try {
            return CURATOR_FRAMEWORK.getChildren()
                    .forPath("/" + ROOT_PATH)
                    .stream()
                    .map(s -> {
                        try {
                            byte[] bytes = CURATOR_FRAMEWORK.getData().forPath("/" + ROOT_PATH + "/" + s);
                            return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), RegisterDataxInfo.class);
                        } catch (Exception e) {
                            LOG.error("refresh error");
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("register server error");
            throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, "DataX Server注册失败", e);
        }
    }

    private static String getZkAddress() {
        Properties properties = loadZkServerProperties();
        String zkAddress = properties.getProperty("registry.zookeeper.address");
        if (StringUtils.isEmpty(zkAddress)) {
            throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, "server.properties未配置registry.zookeeper.address");
        }
        return zkAddress;
    }

    public static int getWeight() {
        Properties properties = loadZkServerProperties();
        String weight = properties.getProperty("weight");
        if (StringUtils.isEmpty(weight)) {
            return 1;
        }
        return Integer.parseInt(weight);
    }

    private static Properties loadZkServerProperties() {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(CoreConstant.DATAX_SERVER_CONF_PATH)))) {
            Properties properties = new Properties();
            properties.load(bufferedReader);
            return properties;
        } catch (IOException e) {
            LOG.error("load server.properties error");
            throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, "加载server.properties出错.", e);
        }
    }
}
