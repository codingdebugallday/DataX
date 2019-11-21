package org.abigballofmud.datax.plugin.reader.otsplusreader.adaptor;

import java.lang.reflect.Type;

import com.aliyun.openservices.ots.model.ColumnType;
import com.google.gson.*;
import org.abigballofmud.datax.plugin.reader.otsplusreader.model.OTSColumn;
import org.apache.commons.codec.binary.Base64;

/**
 * <p>description</p>
 *
 * @author abigballofmud 2019-09-06 16:44:09
 */
public class OTSColumnAdaptor implements JsonDeserializer<OTSColumn>, JsonSerializer<OTSColumn> {
    private static final String NAME = "name";
    private static final String COLUMN_TYPE = "column_type";
    private static final String VALUE_TYPE = "value_type";
    private static final String VALUE = "value";
    private static final String DECODE = "decode";
    private static final String ENCODE = "encode";

    private void serializeConstColumn(JsonObject json, OTSColumn obj) {
        switch (obj.getValueType()) {
            case STRING:
                json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.STRING.toString()));
                json.add(VALUE, new JsonPrimitive(obj.getValue().asString()));
                break;
            case INTEGER:
                json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.INTEGER.toString()));
                json.add(VALUE, new JsonPrimitive(obj.getValue().asLong()));
                break;
            case DOUBLE:
                json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.DOUBLE.toString()));
                json.add(VALUE, new JsonPrimitive(obj.getValue().asDouble()));
                break;
            case BOOLEAN:
                json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.BOOLEAN.toString()));
                json.add(VALUE, new JsonPrimitive(obj.getValue().asBoolean()));
                break;
            case BINARY:
                json.add(VALUE_TYPE, new JsonPrimitive(ColumnType.BINARY.toString()));
                json.add(VALUE, new JsonPrimitive(Base64.encodeBase64String(obj.getValue().asBytes())));
                break;
            default:
                throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getValueType() + "");
        }
    }

    private OTSColumn deserializeConstColumn(JsonObject obj) {
        String strType = obj.getAsJsonPrimitive(VALUE_TYPE).getAsString();
        ColumnType type = ColumnType.valueOf(strType);

        JsonPrimitive jsonValue = obj.getAsJsonPrimitive(VALUE);
        switch (type) {
            case STRING:
                return OTSColumn.fromConstStringColumn(jsonValue.getAsString());
            case INTEGER:
                return OTSColumn.fromConstIntegerColumn(jsonValue.getAsLong());
            case DOUBLE:
                return OTSColumn.fromConstDoubleColumn(jsonValue.getAsDouble());
            case BOOLEAN:
                return OTSColumn.fromConstBoolColumn(jsonValue.getAsBoolean());
            case BINARY:
                return OTSColumn.fromConstBytesColumn(Base64.decodeBase64(jsonValue.getAsString()));
            default:
                throw new IllegalArgumentException("Unsupport deserialize the type : " + type + "");
        }
    }

    private void serializeNormalColumn(JsonObject json, OTSColumn obj) {
        json.add(NAME, new JsonPrimitive(obj.getName()));
        if (obj.getDecode() != null) {
            json.add(DECODE, new JsonPrimitive(obj.getDecode()));
        }
        if (obj.getEncode() != null) {
            json.add(ENCODE, new JsonPrimitive(obj.getEncode()));
        }
    }

    private OTSColumn deserializeNormarlColumn(JsonObject obj) {
        OTSColumn otsColumn = OTSColumn.fromNormalColumn(obj.getAsJsonPrimitive(NAME).getAsString());
        if (obj.getAsJsonPrimitive(DECODE) != null) {
            otsColumn.setDecode(obj.getAsJsonPrimitive(DECODE).getAsString());
        }
        if (obj.getAsJsonPrimitive(ENCODE) != null) {
            otsColumn.setEncode(obj.getAsJsonPrimitive(ENCODE).getAsString());
        }
        return otsColumn;
    }

    @Override
    public JsonElement serialize(OTSColumn obj, Type t,
                                 JsonSerializationContext c) {
        JsonObject json = new JsonObject();

        switch (obj.getColumnType()) {
            case CONST:
                json.add(COLUMN_TYPE, new JsonPrimitive(OTSColumn.OTSColumnType.CONST.toString()));
                serializeConstColumn(json, obj);
                break;
            case NORMAL:
                json.add(COLUMN_TYPE, new JsonPrimitive(OTSColumn.OTSColumnType.NORMAL.toString()));
                serializeNormalColumn(json, obj);
                break;
            default:
                throw new IllegalArgumentException("Unsupport serialize the type : " + obj.getColumnType() + "");
        }
        return json;
    }

    @Override
    public OTSColumn deserialize(JsonElement ele, Type t,
                                 JsonDeserializationContext c) throws JsonParseException {
        JsonObject obj = ele.getAsJsonObject();
        String strColumnType = obj.getAsJsonPrimitive(COLUMN_TYPE).getAsString();
        OTSColumn.OTSColumnType columnType = OTSColumn.OTSColumnType.valueOf(strColumnType);

        switch (columnType) {
            case CONST:
                return deserializeConstColumn(obj);
            case NORMAL:
                return deserializeNormarlColumn(obj);
            default:
                throw new IllegalArgumentException("Unsupport deserialize the type : " + columnType + "");
        }
    }
}
