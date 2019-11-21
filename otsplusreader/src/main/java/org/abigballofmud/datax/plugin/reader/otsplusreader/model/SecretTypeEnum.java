package org.abigballofmud.datax.plugin.reader.otsplusreader.model;

/**
 * <p>
 * switch在jdk6不支持string等，故采用枚举适配下
 * </p>
 *
 * @author isacc 2019/09/09 14:51
 * @since 1.0
 */
public enum SecretTypeEnum {

    /**
     * BASE64
     */
    BASE64(1, "BASE64"),

    /**
     * ZLIB + BASE64
     */
    ZLIB_BASE64(2, "ZLIB_BASE64");

    private Integer order;

    private String name;

    SecretTypeEnum(int order, String name) {
        this.order = order;
        this.name = name;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
