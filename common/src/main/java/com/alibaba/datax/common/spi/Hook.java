package com.alibaba.datax.common.spi;

import java.util.Map;

import com.alibaba.datax.common.statistics.JobStatistics;
import com.alibaba.datax.common.util.Configuration;

/**
 * Created by xiafei.qiuxf on 14/12/17.
 */
public interface Hook {

    /**
     * 返回名字
     *
     * @return
     */
    public String getName();

    /**
     * TODO 文档
     *
     * @param jobConf
     * @param msg
     * @param jobStatistics Datax job监控信息
     */
    void invoke(Configuration jobConf, Map<String, Number> msg, JobStatistics jobStatistics);

}
