package org.abigballofmud.datax.plugin.reader.hdfsplusreader;

import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPlusReader extends Reader {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration readerOriginConfig = null;
        private String encoding = null;
        private HashSet<String> sourceFiles;
        private String specifiedFileType = null;
        private DFSUtil dfsUtil = null;
        private List<String> path = null;
        private String querySql = null;

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            dfsUtil = new DFSUtil(this.readerOriginConfig);
            LOG.info("init() ok and end...");
        }

        public void validate() {
            this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HdfsPlusReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            // 判断是hdfs文件模式还是querySql模式
            querySql = this.readerOriginConfig.getString(Key.QUERY_SQL);
            if (StringUtils.isBlank(querySql)) {
                // path check
                String pathInString = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
                if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                    path = new ArrayList<>();
                    path.add(pathInString);
                } else {
                    path = this.readerOriginConfig.getList(Key.PATH, String.class);
                    if (null == path || path.size() == 0) {
                        throw DataXException.asDataXException(HdfsPlusReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                    }
                    for (String eachPath : path) {
                        if (!eachPath.startsWith("/")) {
                            String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsPlusReaderErrorCode.ILLEGAL_VALUE, message);
                        }
                    }
                }

                specifiedFileType = this.readerOriginConfig.getNecessaryValue(Key.FILETYPE, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
                if (!specifiedFileType.equalsIgnoreCase(Constant.ORC) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.TEXT) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.CSV) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.SEQ) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.RC)) {
                    String message = "HdfsReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                            "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
                    throw DataXException.asDataXException(HdfsPlusReaderErrorCode.FILE_TYPE_ERROR, message);
                }

                encoding = this.readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");

                try {
                    Charsets.toCharset(encoding);
                } catch (UnsupportedCharsetException uce) {
                    throw DataXException.asDataXException(
                            HdfsPlusReaderErrorCode.ILLEGAL_VALUE,
                            String.format("不支持的编码格式 : [%s]", encoding), uce);
                } catch (Exception e) {
                    throw DataXException.asDataXException(
                            HdfsPlusReaderErrorCode.ILLEGAL_VALUE,
                            String.format("运行配置异常 : %s", e.getMessage()), e);
                }

                // validate the Columns
                validateColumns();

                if (this.specifiedFileType.equalsIgnoreCase(Constant.CSV)) {
                    //compress校验
                    UnstructuredStorageReaderUtil.validateCompress(this.readerOriginConfig);
                    UnstructuredStorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
                }
            }
            // check Kerberos
            Boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
            }

        }

        private void validateColumns() {

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                readerOriginConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN, new ArrayList<String>());
            } else {
                // column: 1. index type 2.value type 3.when type is Data, may have format
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw DataXException.asDataXException(
                            HdfsPlusReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                if (null != columns && columns.size() != 0) {
                    for (Configuration eachColumnConf : columns) {
                        eachColumnConf.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
                        Integer columnIndex = eachColumnConf.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
                        String columnValue = eachColumnConf.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

                        if (null == columnIndex && null == columnValue) {
                            throw DataXException.asDataXException(
                                    HdfsPlusReaderErrorCode.NO_INDEX_VALUE,
                                    "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnValue) {
                            throw DataXException.asDataXException(
                                    HdfsPlusReaderErrorCode.MIXED_INDEX_VALUE,
                                    "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }

                    }
                }
            }
        }

        @Override
        public void prepare() {
            // 新建临时目录 hdfs dfs -mkdir -p /tmp/datax-hivereader 即Key.TMP_PATH_PREFIX
            if (dfsUtil.isExist(Key.TMP_PATH_PREFIX)) {
                dfsUtil.mkdir(Key.TMP_PATH_PREFIX);
            }
            if (StringUtils.isBlank(querySql)) {
                LOG.info("prepare(), start to getAllFiles...");
                this.sourceFiles = dfsUtil.getAllFiles(path, specifiedFileType);
                LOG.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]",
                        this.sourceFiles.size(),
                        StringUtils.join(this.sourceFiles, ",")));
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfigs;
            if (StringUtils.isBlank(querySql)) {
                LOG.info("split() begin...");
                readerSplitConfigs = new ArrayList<>();
                // warn:每个slice拖且仅拖一个文件,
                // int splitNumber = adviceNumber;
                int splitNumber = this.sourceFiles.size();
                if (0 == splitNumber) {
                    throw DataXException.asDataXException(HdfsPlusReaderErrorCode.EMPTY_DIR_EXCEPTION,
                            String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.readerOriginConfig.getString(Key.PATH)));
                }

                List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList<String>(this.sourceFiles), splitNumber);
                for (List<String> files : splitedSourceFiles) {
                    Configuration splitedConfig = this.readerOriginConfig.clone();
                    splitedConfig.set(Constant.SOURCE_FILES, files);
                    readerSplitConfigs.add(splitedConfig);
                }
            } else {
                LOG.warn("split() unsupported...");
                readerSplitConfigs = Collections.singletonList(readerOriginConfig);
            }
            return readerSplitConfigs;
        }


        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }


        @Override
        public void post() {
            // ignore
        }

        @Override
        public void destroy() {
            // ignore
        }

    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
        private Configuration taskConfig;
        private List<String> sourceFiles;
        private String specifiedFileType;
        private String encoding;
        private DFSUtil dfsUtil = null;
        private int bufferSize;

        // querySql模式

        private String querySql = null;
        private String tmpTableName = null;
        private String tmpPath = null;
        private Set<String> sourceTempFiles = null;
        private static final String DOUBLE_QUOTATION = "\"";

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.taskConfig = super.getPluginJobConf();
            this.querySql = taskConfig.getString(Key.QUERY_SQL);
            if (StringUtils.isBlank(this.querySql)) {
                this.sourceFiles = this.taskConfig.getList(Constant.SOURCE_FILES, String.class);
                this.specifiedFileType = this.taskConfig.getNecessaryValue(Key.FILETYPE, HdfsPlusReaderErrorCode.REQUIRED_VALUE);
                this.encoding = this.taskConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");
                this.bufferSize = this.taskConfig.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BUFFER_SIZE,
                        com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BUFFER_SIZE);
            } else {
                tmpTableName = getTmpTableName();
                tmpPath = getTmpPath(tmpTableName);
            }
            this.dfsUtil = new DFSUtil(this.taskConfig);
            LOG.info("init() ok and end...");
        }

        private String getTmpTableName() {
            String tableName = RandomStringUtils.random(32, true, false);
            return tableName.substring(0, 1).toLowerCase() + tableName.substring(1);
        }

        private String getTmpPath(String tmpTableName) {
            return Key.TMP_PATH_PREFIX + tmpTableName;
        }

        @Override
        public void prepare() {
            LOG.info("prepare() begin...");
            if (!StringUtils.isBlank(querySql)) {
                String hiveCmd = "CREATE TABLE " + tmpTableName + " STORED AS ORC LOCATION '" + tmpPath + "' AS " + querySql;
                LOG.info("prepare() hive cmd: {}", hiveCmd);
                try {
                    if (!ShellUtil.exec(new String[]{"hive", "-e", DOUBLE_QUOTATION + hiveCmd + DOUBLE_QUOTATION})) {
                        throw DataXException.asDataXException(HdfsPlusReaderErrorCode.SHELL_ERROR, "创建hive临时表脚本执行失败");
                    }
                } catch (Exception e) {
                    throw DataXException.asDataXException(HdfsPlusReaderErrorCode.SHELL_ERROR, "创建hive临时表脚本执行失败", e);
                }
                this.sourceTempFiles = dfsUtil.getAllFiles(Collections.singletonList(tmpPath), Constant.ORC);
            }
            LOG.info("prepare() end...");
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            if (StringUtils.isBlank(this.querySql)) {
                for (String sourceFile : this.sourceFiles) {
                    LOG.info("reading file : {}", sourceFile);
                    if (specifiedFileType.equalsIgnoreCase(Constant.TEXT)
                            || specifiedFileType.equalsIgnoreCase(Constant.CSV)) {
                        InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                        UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig,
                                recordSender, this.getTaskPluginCollector());
                    } else if (specifiedFileType.equalsIgnoreCase(Constant.ORC)) {
                        dfsUtil.orcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    } else if (specifiedFileType.equalsIgnoreCase(Constant.SEQ)) {
                        dfsUtil.sequenceFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    } else if (specifiedFileType.equalsIgnoreCase(Constant.RC)) {
                        dfsUtil.rcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    } else {
                        String message = "HdfsReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                                "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
                        throw DataXException.asDataXException(HdfsPlusReaderErrorCode.FILE_TYPE_UNSUPPORT, message);
                    }
                    if (recordSender != null) {
                        recordSender.flush();
                    }
                }
            } else {
                for (String sourceFile : sourceTempFiles) {
                    LOG.info("reading file: {}", sourceFile);
                    dfsUtil.orcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    if (recordSender != null) {
                        recordSender.flush();
                    }
                }
            }
            LOG.info("end read source files...");
        }

        @Override
        public void post() {
            LOG.info("post() begin...");
            if (!StringUtils.isBlank(querySql)) {
                String hiveCmd = "drop table " + tmpTableName;
                LOG.info("post() hive cmd: {}", hiveCmd);
                // 执行脚本，删除临时表
                try {
                    if (!ShellUtil.exec(new String[]{"hive", "-e", DOUBLE_QUOTATION + hiveCmd + DOUBLE_QUOTATION})) {
                        throw DataXException.asDataXException(HdfsPlusReaderErrorCode.SHELL_ERROR, "删除hive临时表脚本执行失败");
                    }
                } catch (Exception e) {
                    throw DataXException.asDataXException(HdfsPlusReaderErrorCode.SHELL_ERROR, "删除hive临时表脚本执行失败", e);
                }
            }
            LOG.info("post() end...");
        }

        @Override
        public void destroy() {
            // ignore
        }

    }

}