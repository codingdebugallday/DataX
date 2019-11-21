package org.abigballofmud.datax.plugin.reader.otsplusreader.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.*;
import org.abigballofmud.datax.plugin.reader.otsplusreader.OtsPlusReaderError;
import org.abigballofmud.datax.plugin.reader.otsplusreader.model.OTSColumn;
import org.abigballofmud.datax.plugin.reader.otsplusreader.model.OTSPrimaryKeyColumn;
import org.abigballofmud.datax.plugin.reader.otsplusreader.model.SecretTypeEnum;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-09-06 16:44:09
 * @since 1.0
 */
public class Common {

    private Common() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(Common.class);

    public static int primaryKeyValueCmp(PrimaryKeyValue v1, PrimaryKeyValue v2) {
        if (v1.getType() != null && v2.getType() != null) {
            if (v1.getType() != v2.getType()) {
                throw new IllegalArgumentException(
                        "Not same column type, column1:" + v1.getType() + ", column2:" + v2.getType());
            }
            switch (v1.getType()) {
                case INTEGER:
                    Long l1 = v1.asLong();
                    Long l2 = v2.asLong();
                    return l1.compareTo(l2);
                case STRING:
                    return v1.asString().compareTo(v2.asString());
                default:
                    throw new IllegalArgumentException("Unsuporrt compare the type: " + v1.getType() + ".");
            }
        } else {
            if (v1 == v2) {
                return 0;
            } else {
                if (v1 == PrimaryKeyValue.INF_MIN) {
                    return -1;
                } else if (v1 == PrimaryKeyValue.INF_MAX) {
                    return 1;
                }

                if (v2 == PrimaryKeyValue.INF_MAX) {
                    return -1;
                } else if (v2 == PrimaryKeyValue.INF_MIN) {
                    return 1;
                }
            }
        }
        return 0;
    }

    public static OTSPrimaryKeyColumn getPartitionKey(TableMeta meta) {
        List<String> keys = new ArrayList<String>();
        keys.addAll(meta.getPrimaryKey().keySet());

        String key = keys.get(0);

        OTSPrimaryKeyColumn col = new OTSPrimaryKeyColumn();
        col.setName(key);
        col.setType(meta.getPrimaryKey().get(key));
        return col;
    }

    public static List<String> getPrimaryKeyNameList(TableMeta meta) {
        List<String> names = new ArrayList<String>();
        names.addAll(meta.getPrimaryKey().keySet());
        return names;
    }

    public static int compareRangeBeginAndEnd(TableMeta meta, RowPrimaryKey begin, RowPrimaryKey end) {
        if (begin.getPrimaryKey().size() != end.getPrimaryKey().size()) {
            throw new IllegalArgumentException("Input size of begin not equal size of end, begin size : " + begin.getPrimaryKey().size() +
                    ", end size : " + end.getPrimaryKey().size() + ".");
        }
        for (String key : meta.getPrimaryKey().keySet()) {
            PrimaryKeyValue v1 = begin.getPrimaryKey().get(key);
            PrimaryKeyValue v2 = end.getPrimaryKey().get(key);
            int cmp = primaryKeyValueCmp(v1, v2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static List<String> getNormalColumnNameList(List<OTSColumn> columns) {
        List<String> normalColumns = new ArrayList<String>();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                normalColumns.add(col.getName());
            }
        }
        return normalColumns;
    }

    private static ColumnValue columnDecodeHandler(Map<String, ColumnValue> values, OTSColumn col) throws IOException {
        ColumnValue v;
        ColumnValue originColumnValue = values.get(col.getName());
        switch (SecretTypeEnum.valueOf(col.getDecode()).getOrder()) {
            case 1:
                // base64解密
                v = ColumnValue.fromString(new String(new BASE64Decoder().decodeBuffer(originColumnValue.asString())));
                break;
            case 2:
                // zlib + base64
                byte[] bytes = new BASE64Decoder().decodeBuffer(originColumnValue.asString());
                v = ColumnValue.fromString(new String(ZlibUtil.decompress(bytes)));
                break;
            default:
                throw DataXException.asDataXException(OtsPlusReaderError.ERROR,
                        String.format("暂不支持此[%s]加密解密方式", col.getDecode()));
        }
        return v;
    }

    private static ColumnValue columnEncodeHandler(Map<String, ColumnValue> values, OTSColumn col) {
        ColumnValue originColumnValue = values.get(col.getName());
        ColumnValue v;
        switch (SecretTypeEnum.valueOf(col.getEncode()).getOrder()) {
            case 1:
                // base64加密
                v = ColumnValue.fromString(new BASE64Encoder().encodeBuffer(originColumnValue.asString().getBytes()));
                break;
            case 2:
                // zlib + base64
                byte[] bytes = ZlibUtil.compress(originColumnValue.asString().getBytes());
                v = ColumnValue.fromString(new BASE64Encoder().encodeBuffer(bytes));
                break;
            default:
                throw DataXException.asDataXException(OtsPlusReaderError.ERROR,
                        String.format("暂不支持此[%s]加密解密方式", col.getDecode()));
        }
        return v;
    }

    public static Record parseRowToLine(Row row, List<OTSColumn> columns, Record line) {
        Map<String, ColumnValue> values = row.getColumns();
        for (OTSColumn col : columns) {
            if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                line.addColumn(col.getValue());
            } else {
                ColumnValue v;
                if (StringUtils.isNotEmpty(col.getDecode()) && StringUtils.isEmpty(col.getEncode())) {
                    // 该字段需要解密
                    try {
                        v = Common.columnDecodeHandler(values, col);
                    } catch (IOException e) {
                        throw new IllegalArgumentException(String.format("column [%s] base64 decode error.", col.getName()));
                    }
                } else if (StringUtils.isNotEmpty(col.getEncode()) && StringUtils.isEmpty(col.getDecode())) {
                    // 该字段需要加密
                    v = columnEncodeHandler(values, col);
                } else {
                    // 该字段不需处理
                    v = values.get(col.getName());
                }
                if (v == null) {
                    line.addColumn(new StringColumn(null));
                } else {
                    switch (v.getType()) {
                        case STRING:
                            line.addColumn(new StringColumn(v.asString()));
                            break;
                        case INTEGER:
                            line.addColumn(new LongColumn(v.asLong()));
                            break;
                        case DOUBLE:
                            line.addColumn(new DoubleColumn(v.asDouble()));
                            break;
                        case BOOLEAN:
                            line.addColumn(new BoolColumn(v.asBoolean()));
                            break;
                        case BINARY:
                            line.addColumn(new BytesColumn(v.asBinary()));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsuporrt tranform the type: " + col.getValue().getType() + ".");
                    }
                }
            }
        }
        return line;
    }

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }

    public static long getDelaySendMillinSeconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            }
        }
        return sleepTime;
    }
}
