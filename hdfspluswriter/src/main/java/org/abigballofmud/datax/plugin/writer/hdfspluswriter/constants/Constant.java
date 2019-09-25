package org.abigballofmud.datax.plugin.writer.hdfspluswriter.constants;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-9-25 14:01:31
 */
public final class Constant {

    private Constant() {
        throw new IllegalStateException("constant class!");
    }

    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_NULL_FORMAT = "\\N";
    public static final String DOUBLE_QUOTATION = "\"";
    public static final String TMP_FILE_PATH = "%s__%s%s";
    public static final String FULL_FILE_PATH = "%s%s%s__%s";

}
