package com.alibaba.datax.app.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.HttpHeaderNames;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * description
 *
 * @author thestyleofme 2021/01/18 11:06 上午
 */
public class HttpProvider {

    private static final Logger LOG = LoggerFactory.getLogger(HttpProvider.class);

    static OkHttpClient okHttpClient;

    static {
        okHttpClient = new OkHttpClient.Builder()
                // 设置连接超时时间
                .connectTimeout(30, TimeUnit.SECONDS)
                // 设置读取超时时间
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    private HttpProvider() {
    }

    public static OkHttpClient getClient() {
        return okHttpClient;
    }

    public static boolean ping(String url) {
        Request request = new Request.Builder()
                .url(url)
                .addHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "application/json; charset=UTF-8")
                .build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            return response.isSuccessful();
        } catch (IOException e) {
            LOG.error("ping datax server error", e);
            return false;
        }
    }

    public static void main(String[] args) {
        boolean ping = HttpProvider.ping("http://172.23.16.76:26301/index");
        LOG.info("ping server status: {}", ping);
    }
}
