package com.alibaba.datax.app.server.handler;

import com.alibaba.datax.app.client.UrlHandler;
import com.alibaba.datax.app.pojo.Result;
import com.alibaba.datax.app.utils.UrlHandlerUtil;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/11 1:31
 * @since 1.0.0
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        // 100-continue
        if (HttpUtil.is100ContinueExpected(request)) {
            ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }
        // 找合适的handler类进行处理
        String uri = request.uri();
        String requestMethod = request.method().name();
        UrlHandler urlHandler = UrlHandlerUtil.getUrlHandler(uri, requestMethod);
        Result<?> result = urlHandler != null ? urlHandler.handle(request) :
                Result.fail("404 - Not Found - 找不到当前url的对应处理类，请检查 [url] 以及 [请求方法] 是否正确");
        // 创建http响应
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(JSON.toJSONString(result), CharsetUtil.UTF_8));
        // 设置头信息 若返回页面 text/html; charset=UTF-8
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        // 将返回信息 write到客户端
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

}
