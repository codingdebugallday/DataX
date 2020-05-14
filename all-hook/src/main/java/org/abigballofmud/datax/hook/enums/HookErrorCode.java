package org.abigballofmud.datax.hook.enums;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * <p>
 * HookErrorCode
 * </p>
 *
 * @author isacc 2020/5/13 19:40
 * @since 1.0
 */
public enum  HookErrorCode implements ErrorCode {

    /**
     * 加载statistics.properties出错
     */
    LOAD_STATISTICS_PROPERTIES_ERROR("HookErrCode-01","加载statistics.properties出错");

    private final String code;

    private final String description;

    HookErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
