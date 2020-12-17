package com.alibaba.datax.app.client.handler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.alibaba.datax.app.client.RequestMapping;
import com.alibaba.datax.app.client.UrlHandler;
import com.alibaba.datax.app.context.JobLogCollectorContext;
import com.alibaba.datax.app.pojo.DataxJobInfo;
import com.alibaba.datax.app.pojo.Result;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson.JSON;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * /data/job POST
 * </p>
 *
 * @author isaac 2020/12/11 10:51
 * @since 1.0.0
 */
@RequestMapping(value = "/datax/job", method = "POST")
public class DataxJobUrlHandler implements UrlHandler {

    private static final int KILLED_EXIT_CODE = 143;
    private static final int FAIL_EXIT_CODE = 1;

    @SuppressWarnings("unchecked")
    @Override
    public Result<String> handle(FullHttpRequest request) {
        try {
            JobLogCollectorContext.setLogCollector(new StringBuilder());
            String body = request.content().toString(CharsetUtil.UTF_8);
            DataxJobInfo dataxJobInfo = JSON.parseObject(body, DataxJobInfo.class);
            if (StringUtils.isNotEmpty(dataxJobInfo.getJobJson())) {
                // jobJson优先级高 根据json生成临时文件
                return doDataxJobWithJson(dataxJobInfo);
            }
            return doDataxJob(dataxJobInfo);
        } finally {
            JobLogCollectorContext.clear();
        }
    }

    private Result<String> doDataxJob(DataxJobInfo dataxJobInfo) {
        String[] params = genJobParamArray(dataxJobInfo);
        return doDataxJob(params);
    }

    private Result<String> doDataxJobWithJson(DataxJobInfo dataxJobInfo) {
        File temp = new File("tmp/" + UUID.randomUUID().toString() + ".json");
        try {
            FileUtils.writeStringToFile(temp, dataxJobInfo.getJobJson(), StandardCharsets.UTF_8);
            dataxJobInfo.setJob(temp.getAbsolutePath());
            return doDataxJob(dataxJobInfo);
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.JSON_FILE_ERROR,
                    "DataX基于配置的jobJson创建json文件失败", e);
        } finally {
            try {
                FileUtils.forceDelete(temp);
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private Result<String> doDataxJob(String[] params) {
        int exitCode = Engine.doMain(params);
        if (exitCode == FAIL_EXIT_CODE) {
            // 1 失败
            return Result.fail("fail", JobLogCollectorContext.current().toString());
        } else if (exitCode == KILLED_EXIT_CODE) {
            // 143 Killed
            return Result.fail("killed", JobLogCollectorContext.current().toString());
        }
        // 0 正常执行完
        return Result.ok(JobLogCollectorContext.current().toString());
    }

    private String[] genJobParamArray(DataxJobInfo dataxJobInfo) {
        String[] params = new String[6];
        params[0] = "-mode";
        params[1] = dataxJobInfo.getMode();
        params[2] = "-jobid";
        params[3] = String.valueOf(dataxJobInfo.getJobId());
        params[4] = "-job";
        params[5] = dataxJobInfo.getJob();
        return params;
    }
}
