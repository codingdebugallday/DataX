package org.abigballofmud.datax.plugin.writer.hdfspluswriter.constants;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-9-25 14:01:31
 */
public final class Key {

    private Key() {
        throw new IllegalStateException("constant class!");
    }

    /**
     * must have
     */
    public static final String PATH = "path";
    /**
     * must have
     */
    public static final String DEFAULT_FS = "defaultFS";
    /**
     * must have
     */
    public static final String FILE_TYPE = "fileType";
    /**
     * must have
     */
    public static final String FILE_NAME = "fileName";
    /**
     * must have for column
     */
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";

    /**
     * not must
     */
    public static final String PRE_SQL = "preSql";
    /**
     * not must
     */
    public static final String POST_SQL = "postSql";
    /**
     * not must
     * 删除字段中的\n, \r, and \01
     */
    public static final String DROP_IMPORT_DELIMS = "dropImportDelims";
    /**
     * not must
     * 替换字段中的\n, \r, and \01
     */
    public static final String DELIMS_REPLACEMENT = "delimsReplacement";

    /**
     * must have
     */
    public static final String WRITE_MODE = "writeMode";
    /**
     * must have
     */
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    /**
     * not must, default UTF-8
     */
    public static final String ENCODING = "encoding";
    /**
     * not must, default no compress
     */
    public static final String COMPRESS = "compress";
    /**
     * not must, not default \N
     */
    public static final String NULL_FORMAT = "nullFormat";
    /**
     * Kerberos
     */
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    /**
     * hadoop config
     */
    public static final String HADOOP_CONFIG = "hadoopConfig";
}
