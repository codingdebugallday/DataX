package org.abigballofmud.datax.plugin.reader.hdfsplusreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * description
 * </p>
 *
 * @author abigballofmud 2019/09/25 15:06
 * @since 1.0
 */
public class ShellUtil {

    private static final int SUCCESS = 0;
    private static final Logger LOG = LoggerFactory.getLogger(ShellUtil.class);

    private ShellUtil() throws IllegalAccessException {
        throw new IllegalAccessException("Illegal Access!");
    }

    public static boolean exec(String[] command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);
        read(process.getInputStream());
        StringBuilder errMsg = read(process.getErrorStream());
        String commandStr = Arrays.toString(command);
        // 等待程序执行结束并输出状态
        int exitCode = process.waitFor();
        if (exitCode == SUCCESS) {
            LOG.info("command exec successful, command: {}", commandStr);
            return true;
        } else {
            LOG.info("command exec failed, error: {}", errMsg);
            return false;
        }
    }

    private static StringBuilder read(InputStream inputStream) throws IOException {
        StringBuilder resultMsg = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                resultMsg.append(line);
                resultMsg.append("\r\n");
            }
            return resultMsg;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    LOG.error("close inputStream error", e);
                }
            }
        }
    }
}
