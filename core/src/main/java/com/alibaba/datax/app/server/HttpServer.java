package com.alibaba.datax.app.server;

import java.net.*;
import java.util.Enumeration;

import com.alibaba.datax.app.server.handler.HttpRequestHandler;
import com.alibaba.datax.app.server.register.RegisterDataxInfo;
import com.alibaba.datax.app.server.register.ZookeeperRegister;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 1:19
 * @since 1.0.0
 */
public class HttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);

    private final int port;

    public HttpServer(int port) {
        this.port = port;
    }

    public void start() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            ChannelPipeline pipeline = channel.pipeline();
                            // http 编解码
                            pipeline.addLast(new HttpServerCodec());
                            // http 消息聚合器
                            pipeline.addLast("httpAggregator", new HttpObjectAggregator(512 * 1024));
                            // 请求处理器
                            pipeline.addLast(new HttpRequestHandler());
                        }
                    });
            ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(port)).sync();
            int realPort = ((NioServerSocketChannel) channelFuture.channel()).localAddress().getPort();
            register(realPort);
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }

    private void register(int realPort) {
        String ip = getLocalIp();
        LOG.debug("DataX server: {}:{}", ip, realPort);
        // 封装注册信息
        RegisterDataxInfo registerDataxInfo = new RegisterDataxInfo();
        // url
        registerDataxInfo.setUrl(String.format("http://%s:%d", ip, realPort));
        // weight 可在server.properties配置 默认为1
        registerDataxInfo.setWeight(ZookeeperRegister.getWeight());
        // 注册到zk
        ZookeeperRegister.register(JSON.toJSONString(registerDataxInfo));
    }

    private String getLocalIp() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, e);
        }
        throw DataXException.asDataXException(FrameworkErrorCode.ZK_REGISTER_FAILED, "找不到合适的网卡");
    }
}