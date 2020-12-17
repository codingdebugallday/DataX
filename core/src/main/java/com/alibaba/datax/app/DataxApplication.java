package com.alibaba.datax.app;

import com.alibaba.datax.app.server.HttpServer;
import com.alibaba.datax.app.utils.UrlHandlerUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 1:36
 * @since 1.0.0
 */
public class DataxApplication {

    /**
     * 注意：datax.home环境变量必须配置
     * <p>
     * 可设置vm参数
     * -Dfile.encoding=UTF-8
     * -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener
     * -Ddatax.home=E:/myGitCode/MyDatax/target/datax/datax
     * -Dlogback.configurationFile=E:/myGitCode/MyDatax/core/src/main/conf/logback.xml
     * <p>
     * 也可程序指定
     * System.setProperty("datax.home", "E:/myGitCode/MyDatax/target/datax/datax")
     */
    public static void main(String[] args) {
        // 扫描@RequestMapping IOC
        UrlHandlerUtil.doInstance(DataxApplication.class.getPackage().getName());
        HttpServer server = new HttpServer();
        server.start();
    }
}
