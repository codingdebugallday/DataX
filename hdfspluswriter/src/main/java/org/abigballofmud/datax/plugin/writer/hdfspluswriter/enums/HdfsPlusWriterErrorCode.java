package org.abigballofmud.datax.plugin.writer.hdfspluswriter.enums;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-9-25 14:01:31
 */
public enum HdfsPlusWriterErrorCode implements ErrorCode {
    /**
     * 常用错误信息
     */
    CONFIG_INVALID_EXCEPTION("HdfsPlusWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("HdfsPlusWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("HdfsPlusWriter-02", "您填写的参数值不合法."),
    WRITER_FILE_WITH_CHARSET_ERROR("HdfsPlusWriter-03", "您配置的编码未能正常写入."),
    WRITE_FILE_IO_ERROR("HdfsPlusWriter-04", "您配置的文件在写入时出现IO异常."),
    WRITER_RUNTIME_EXCEPTION("HdfsPlusWriter-05", "出现运行时异常, 请联系我们."),
    CONNECT_HDFS_IO_ERROR("HdfsPlusWriter-06", "与HDFS建立连接时出现IO异常."),
    COLUMN_REQUIRED_VALUE("HdfsPlusWriter-07", "您column配置中缺失了必须填写的参数值."),
    HDFS_RENAME_FILE_ERROR("HdfsPlusWriter-08", "将文件移动到配置路径失败."),
    KERBEROS_LOGIN_ERROR("HdfsPlusWriter-09", "KERBEROS认证失败"),
    HIVE_SQL_EXEC_ERROR("HdfsPlusWriter-10", "HIVE SQL执行出错");

    private final String code;
    private final String description;

    private HdfsPlusWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
