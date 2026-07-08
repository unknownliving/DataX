package com.alibaba.datax.plugin.reader.rdbmswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SubCommonRdbmsWriter extends CommonRdbmsWriter {
    static {
        DBUtil.loadDriverClass("writer", "rdbms");
    }

    public static class Job extends CommonRdbmsWriter.Job {
        public Job(DataBaseType dataBaseType) {
            super(dataBaseType);
        }
    }

    public static class Task extends CommonRdbmsWriter.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private boolean identity = false;

        private boolean disableTrigger = false;

        private List<String> customPks;

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        public void init(Configuration writerSliceConfig) {
            super.init(writerSliceConfig);
            this.writeRecordSql = null;
            identity = writerSliceConfig.getBool("identity", false);
            disableTrigger = writerSliceConfig.getBool("disableTrigger", false);
            customPks = writerSliceConfig.getList("customPks", new ArrayList<>(), String.class);
        }

        @Override
        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector, Connection connection) {
            this.taskPluginCollector = taskPluginCollector;

            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                    this.table, StringUtils.join(this.columns, ","));
            // 写数据库的SQL语句
//            rewriteSql(connection);

            if (identity) {
                // 开启自增列插入
                enableIdentityInsert(connection, true);
            }

            if (disableTrigger) {
                // 禁止触发器
                alterDisableTrigger(connection, true);
            }

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                if (identity) {
                    enableIdentityInsert(connection, false);
                }
                if (disableTrigger) {
                    alterDisableTrigger(connection, false);
                }
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        private void alterDisableTrigger(Connection connection, boolean disable) {
            String sql = disable ?
                    "ALTER TABLE " + this.table + " DISABLE ALL TRIGGERS" :
                    "ALTER TABLE " + this.table + " ENABLE ALL TRIGGERS";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.execute();
                LOG.info("{} 表的触发器：{}", this.table, disable ? "已关闭" : "已开启");
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        protected void doBatchInsert(Connection connection, List<Record> buffer)
                throws SQLException {
            rewriteSql(connection, buffer.size());
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);
                if (writeMode.trim().toLowerCase().startsWith("insert")) {
                    for (Record record : buffer) {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                } else {
                    // 2. 初始化JDBC参数索引（JDBC参数从1开始，不是0！）
                    int paramIndex = 1;

                    // 3. 遍历每一条Record，批量填充参数
                    for (Record record : buffer) {
                        // 遍历当前Record的所有列（和原有单条逻辑一致）
                        for (int i = 0; i < this.columnNumber; i++) {
                            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                            String typeName = this.resultSetMetaData.getRight().get(i);
                            // 核心：复用原有列类型填充逻辑，参数索引改为累加的paramIndex
                            preparedStatement = fillPreparedStatementColumnType(
                                    preparedStatement,
                                    paramIndex - 1, // 注意：如果fillPreparedStatementColumnType内部用0开始的索引，需减1；如果直接用1开始的参数位，直接传paramIndex
                                    columnSqltype,
                                    typeName,
                                    record.getColumn(i)
                            );
                            // 参数索引自增，为下一列做准备
                            paramIndex++;
                        }
                    }
                    preparedStatement.executeUpdate();
                }
                connection.commit();
            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        /**
         * 开启/关闭自增列手动插入权限
         * @param enable true=开启，false=关闭
         */
        private void enableIdentityInsert(Connection connection, boolean enable) {
            String sql = enable ?
                    "SET IDENTITY_INSERT " + this.table + " ON" :
                    "SET IDENTITY_INSERT " + this.table + " OFF";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.execute();
                LOG.info("{} 表的IDENTITY_INSERT：{}", this.table, enable ? "已开启" : "已关闭");
            } catch (SQLException e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        private void rewriteSql(Connection connection, int recordSize) {
            if (writeMode.trim().toLowerCase().startsWith("insert") && this.writeRecordSql != null) {
                return;
            }
            if (recordSize == batchSize && this.writeRecordSql != null) {
                return;
            }
            List<String> valueHolders = new ArrayList<>(columnNumber);
            for (int i = 0; i < columns.size(); i++) {
                String type = resultSetMetaData.getRight().get(i);
                valueHolders.add(calcValueHolder(type));
            }

            if (writeMode.trim().toLowerCase().startsWith("insert")) {
                String writeDataSqlTemplate = new StringBuilder()
                        .append("INSERT INTO %s (").append(StringUtils.join(this.columns, ","))
                        .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                        .append(")").toString();
                this.writeRecordSql = String.format(writeDataSqlTemplate, this.table);
                return;
            }
            ResultSet rs = null;
            try {
                DatabaseMetaData metaData = connection.getMetaData();
                rs = metaData.getPrimaryKeys(null, null, this.table);
                LOG.info("表{}主键信息：", this.table);
                List<String> pkCols = new ArrayList<>();
                while (rs.next()) {
                    String pkCol = rs.getString("COLUMN_NAME");
                    pkCols.add(pkCol);
                }
                List<String> finalCols;
                if (!pkCols.isEmpty()) {
                    finalCols = pkCols;
                } else {
                    finalCols = customPks;
                }
                LOG.info("主键列：{}", finalCols);

                String unionAllSql = getUnionAllSql(recordSize, pkCols, valueHolders);

                // 3. 构建MERGE INTO语句核心部分
                // 3.1 构建匹配条件（主键相等）
                StringBuilder onCondition = new StringBuilder();
                for (int i = 0; i < finalCols.size(); i++) {
                    String pkCol = finalCols.get(i);
                    if (i > 0) {
                        onCondition.append(" AND ");
                    }
                    // 达梦MERGE INTO中，USING的临时表别名t，目标表别名tgt
                    onCondition.append("tgt.").append(pkCol).append(" = t.").append(pkCol);
                }
                // 3.2 构建UPDATE SET部分（更新非主键列）
                StringBuilder updateSet = new StringBuilder();
                for (int i = 0; i < columns.size(); i++) {
                    String col = columns.get(i);
                    // 主键列不需要更新，跳过
                    if (finalCols.contains(col)) {
                        continue;
                    }
                    if (updateSet.length() > 0) {
                        updateSet.append(", ");
                    }
                    updateSet.append(col).append(" = t.").append(col);
                }

                // 达梦MERGE INTO语法：MERGE INTO 目标表 tgt USING (VALUES(占位符)) t(列名) ON (匹配条件)
                // WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT(列名) VALUES(占位符)
                // 5. 拼接完整批量MERGE INTO SQL
                StringBuilder mergeSql = new StringBuilder();
                mergeSql.append("MERGE INTO ").append(table).append(" tgt ")
                        .append("USING (").append(unionAllSql).append(") t ")
                        .append("ON (").append(onCondition).append(") ")
                        .append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ")
                        .append("WHEN NOT MATCHED THEN INSERT (").append(StringUtils.join(columns, ","))
                        .append(") VALUES (").append(StringUtils.join(columns.stream().map(col -> "t." + col).toArray(), ","))
                        .append(")");

                this.writeRecordSql = mergeSql.toString();
                LOG.info("writeRecordSql: {}", this.writeRecordSql);
            } catch (SQLException e) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
                                String.format("获取表:%s 的主键元信息时失败. 请联系 DBA 核查该库、表信息.", this.table), e);
            } finally {
                DBUtil.closeDBResources(rs, null, null);
            }
        }

        private String getUnionAllSql(int recordSize, List<String> pkCols, List<String> valueHolders) {
            if (pkCols.isEmpty() && customPks.isEmpty()) {
                throw new IllegalArgumentException("表" + this.table + "未配置主键，无法构建MERGE INTO语句");
            }

            // 为每个?占位符生成带别名的子句（? AS 列名）
            // 例如：? AS id, ? AS name, ? AS age
            List<String> placeholderWithAlias = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                String placeholder = valueHolders.get(i);
                String column = columns.get(i);
                placeholderWithAlias.add(placeholder + " AS " + column);
            }
            String singleRowSql = "SELECT " + StringUtils.join(placeholderWithAlias, ",") + " FROM DUAL";

            List<String> batchRows = new ArrayList<>();
            for (int i = 0; i < recordSize; i++) {
                batchRows.add(singleRowSql);
            }
            return StringUtils.join(batchRows, " UNION ALL ");
        }

        @Override
        protected PreparedStatement fillPreparedStatementColumnType(
                PreparedStatement preparedStatement, int columnIndex,
                int columnSqltype, String typeName, Column column) throws SQLException {
            java.util.Date utilDate;
            try {
                switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.NVARCHAR:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1,
                                column.asString());
                    }
                    break;
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setCharacterStream(columnIndex + 1,
                                new StringReader(column.asString()));
                    }
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.TINYINT:
                    String strLongValue = column.asString();
                    if (emptyAsNull && "".equals(strLongValue)) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setLong(columnIndex + 1,
                                column.asLong());
                    }
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setDouble(columnIndex + 1,
                                column.asDouble());
                    }
                    break;

                case Types.DATE:
                    java.sql.Date sqlDate = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    } else {
                        preparedStatement.setNull(columnIndex + 1, Types.DATE);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                        preparedStatement.setTime(columnIndex + 1, sqlTime);
                    } else {
                        preparedStatement.setNull(columnIndex + 1, Types.TIME);
                    }
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                        preparedStatement.setTimestamp(columnIndex + 1,
                                sqlTimestamp);
                    } else {
                        preparedStatement.setNull(columnIndex + 1,
                                Types.TIMESTAMP);
                    }
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else {
                        preparedStatement.setBytes(columnIndex + 1,
                                column.asBytes());
                    }
                    break;

                case Types.BOOLEAN:
                    if (null == column.getRawData()) {
                        preparedStatement.setNull(columnIndex + 1,
                                Types.BOOLEAN);
                    } else {
                        preparedStatement.setBoolean(columnIndex + 1,
                                column.asBoolean());
                    }
                    break;

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (null == column.getRawData()) {
                        preparedStatement.setObject(columnIndex + 1, null);
                    } else if (this.dataBaseType == DataBaseType.MySql) {
                        preparedStatement.setBoolean(columnIndex + 1,
                                column.asBoolean());
                    } else {
                        preparedStatement.setString(columnIndex + 1,
                                column.asString());
                    }
                    break;
                default:
                    preparedStatement.setObject(columnIndex + 1,
                            column.getRawData());
                    break;
                }
            } catch (DataXException e) {
                throw new SQLException(String.format(
                        "类型转换错误:[%s] 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s].",
                        column,
                        this.resultSetMetaData.getLeft().get(columnIndex),
                        this.resultSetMetaData.getMiddle().get(columnIndex),
                        this.resultSetMetaData.getRight().get(columnIndex)));
            }
            return preparedStatement;
        }
    }
}
