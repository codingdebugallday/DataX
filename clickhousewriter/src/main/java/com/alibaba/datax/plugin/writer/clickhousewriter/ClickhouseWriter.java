package com.alibaba.datax.plugin.writer.clickhousewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * clickhouse writer
 * @author dengzhilong
 */
public class ClickhouseWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.CLICKHOUSE;

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterJob;
		private static final Logger LOG = LoggerFactory
				.getLogger(Job.class);

		@Override
		public void preCheck(){
			this.init();
			this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
		}

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			originalConfig.getUnnecessaryValue(Key.USERNAME, "",DBUtilErrorCode.MYSQL_CONN_USERPWD_ERROR);
			originalConfig.getUnnecessaryValue(Key.PASSWORD, "",DBUtilErrorCode.MYSQL_CONN_USERPWD_ERROR);
			OriginalConfPretreatmentUtil.doCheckBatchSize(originalConfig);
			OriginalConfPretreatmentUtil.simplifyConf(originalConfig);
			OriginalConfPretreatmentUtil.dealColumnConf(originalConfig);
			OriginalConfPretreatmentUtil.dealWriteMode(originalConfig, DATABASE_TYPE);
			LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
					originalConfig.toJSON());
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterJob.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterTask;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
			this.commonRdbmsWriterTask.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
		}

		//TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
					super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterTask.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
		}

		@Override
		public boolean supportFailOver(){
			String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
			return "replace".equalsIgnoreCase(writeMode);
		}

	}


}
