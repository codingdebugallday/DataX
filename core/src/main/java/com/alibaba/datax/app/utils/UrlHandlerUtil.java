package com.alibaba.datax.app.utils;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.alibaba.datax.app.client.RequestMapping;
import com.alibaba.datax.app.client.UrlHandler;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 11:16
 * @since 1.0.0
 */
public class UrlHandlerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(UrlHandlerUtil.class);

    private UrlHandlerUtil() {
    }

    private static final List<String> CLASS_NAMES = new CopyOnWriteArrayList<>();
    private static final Map<String, UrlHandler> IOC = new ConcurrentHashMap<>(16);

    public static UrlHandler getUrlHandler(String url, String method) {
        return IOC.get(url + "_" + method);
    }

    public static UrlHandler getUrlHandler(String url) {
        return getUrlHandler(url, HttpMethod.GET.name());
    }

    private static void doScan(String scanPackage) {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        String classpath = Objects.requireNonNull(resource).getPath();
        String scanPackagePath = classpath + scanPackage.replace(".", "/");
        File packageFile = new File(scanPackagePath);
        File[] files = packageFile.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        for (File file : Objects.requireNonNull(files)) {
            if (file.isDirectory()) {
                doScan(scanPackage + "." + file.getName());
            } else if (file.getName().endsWith(".class")) {
                String className = scanPackage + "." + file.getName().replace(".class", "");
                CLASS_NAMES.add(className);
            }
        }
    }

    public static void doInstance(String scanPackage) {
        doScan(scanPackage);
        if (CLASS_NAMES.isEmpty()) {
            return;
        }
        for (String className : CLASS_NAMES) {
            try {
                Class<?> clazz = Class.forName(className);
                if (clazz.isAnnotationPresent(RequestMapping.class)) {
                    RequestMapping requestMapping = clazz.getAnnotation(RequestMapping.class);
                    String url = requestMapping.value();
                    String method = requestMapping.method();
                    String beanName = url + "_" + method;
                    Object instance = Thread.currentThread()
                            .getContextClassLoader()
                            .loadClass(clazz.getName())
                            .getDeclaredConstructor()
                            .newInstance();
                    IOC.put(beanName, (UrlHandler) instance);
                    LOG.debug("add handler, url: [{}], method: [{}]", url, method);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(FrameworkErrorCode.DO_INSTANCE_ERROR, "DataX URL处理类IOC失败");
            }
        }
    }
}
