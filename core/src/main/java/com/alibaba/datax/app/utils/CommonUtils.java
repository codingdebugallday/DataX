package com.alibaba.datax.app.utils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/17 17:11
 * @since 1.0.0
 */
public class CommonUtils {

    private CommonUtils() {
    }

    /**
     * 获取原始的错误信息，如果没有cause则返回当前message
     *
     * @param e Exception
     * @return 错误信息
     */
    public static String getMessage(Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            return e.getMessage();
        }
        return cause.getMessage();
    }
}
