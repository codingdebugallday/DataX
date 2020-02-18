package com.alibaba.datax.plugin.util;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.rdbmswriter.RdbmsWriter;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class TestConnection {

    @Test
    public void testConnection() throws SQLException {
        String jdbcUrl = "jdbc:sap://192.168.12.23:39017?schema=SYSTEM";
        String dataxHome  = "D:\\projects\\HDSP\\DataX\\target\\datax\\datax";
        //String dataxHome  = "D:\\datax";

        System.setProperty("datax.home","D:\\projects\\HDSP\\DataX\\target\\datax\\datax");
        DBUtil.loadDriverClass("reader", "rdbms");
        Connection connection = DBUtil.getConnection(DataBaseType.RDBMS, jdbcUrl, "system", "HXEHana1");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from  system.HANA_USERINFO");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> resultMap = new HashMap<>(columnCount);
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                resultMap.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
        }
        System.out.println(resultMap);

        resultSet.close();
        preparedStatement.close();
        connection.close();

    }

    @Test
    public void testRdbmsWriter() {
        new RdbmsWriter();
    }
}
