package org.abigballofmud.datax.plugin.reader.otsplusreader.model;

import com.alibaba.datax.common.element.*;
import com.aliyun.openservices.ots.model.ColumnType;

/**
 * <p>OTSColumn</p>
 *
 * @author abigballofmud 2019-09-06 16:44:09
 * @since 1.0
 */
public class OTSColumn {

    private String name;
    /**
     * 解密规则
     */
    private String decode;
    /**
     * 加密规则
     */
    private String encode;
    private Column value;
    private OTSColumnType columnType;
    private ColumnType valueType;

    public static enum OTSColumnType {
        NORMAL, // 普通列
        CONST   // 常量列
    }

    private OTSColumn(String name) {
        this.name = name;
        this.columnType = OTSColumnType.NORMAL;
    }

    private OTSColumn(Column value, ColumnType type) {
        this.value = value;
        this.columnType = OTSColumnType.CONST;
        this.valueType = type;
    }

    public static OTSColumn fromNormalColumn(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        return new OTSColumn(name);
    }

    public static OTSColumn fromDecodeColumn(String name,String decode) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        OTSColumn otsColumn = new OTSColumn(name);
        otsColumn.setDecode(decode);
        return otsColumn;
    }

    public static OTSColumn fromEncodeColumn(String name,String encode) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("The column name is empty.");
        }
        OTSColumn otsColumn = new OTSColumn(name);
        otsColumn.setEncode(encode);
        return otsColumn;
    }

    public static OTSColumn fromConstStringColumn(String value) {
        return new OTSColumn(new StringColumn(value), ColumnType.STRING);
    }

    public static OTSColumn fromConstIntegerColumn(long value) {
        return new OTSColumn(new LongColumn(value), ColumnType.INTEGER);
    }

    public static OTSColumn fromConstDoubleColumn(double value) {
        return new OTSColumn(new DoubleColumn(value), ColumnType.DOUBLE);
    }

    public static OTSColumn fromConstBoolColumn(boolean value) {
        return new OTSColumn(new BoolColumn(value), ColumnType.BOOLEAN);
    }

    public static OTSColumn fromConstBytesColumn(byte[] value) {
        return new OTSColumn(new BytesColumn(value), ColumnType.BINARY);
    }

    public Column getValue() {
        return value;
    }

    public OTSColumnType getColumnType() {
        return columnType;
    }

    public ColumnType getValueType() {
        return valueType;
    }

    public String getName() {
        return name;
    }

    public String getDecode() {
        return decode;
    }

    public String getEncode() {
        return encode;
    }

    public void setDecode(String decode) {
        this.decode = decode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    @Override
    public String toString() {
        return "OTSColumn{" +
                "name='" + name + '\'' +
                ", decode='" + decode + '\'' +
                ", encode='" + encode + '\'' +
                ", value=" + value +
                ", columnType=" + columnType +
                ", valueType=" + valueType +
                '}';
    }
}
