package com.alibaba.datax.plugin.unstructuredstorage.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;

public class SqlWriter implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SqlWriter.class);

    private final Writer sqlWriter;
    private final String quoteChar;
    private final String lineSeparator;
    private final String tableName;
    private StringBuilder insertPrefix;

    public SqlWriter(Writer writer, String quoteChar, String tableName, String lineSeparator, List<String> columnNames) {
        this.sqlWriter = writer;
        this.quoteChar = quoteChar;
        this.lineSeparator = lineSeparator;
        this.tableName = tableName;
        buildInsertPrefix(columnNames);
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
            return;
        }

        String sqlPatten = insertPrefix +
                splitedRows.stream().map(e -> "'" + DataXCsvWriter.replace(e, "'", "''") + "'").collect(Collectors.joining(",")) +
                ");" + lineSeparator;
        this.sqlWriter.write(sqlPatten);
    }

    private void buildInsertPrefix(List<String> columnNames) {
        StringBuilder sb = new StringBuilder(columnNames.size() * 32);

        for (String columnName : columnNames) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(quoteChar).append(columnName).append(quoteChar);
        }

        int capacity = 16 + tableName.length() + sb.length();
        this.insertPrefix = new StringBuilder(capacity);
        this.insertPrefix.append("INSERT INTO ").append(tableName).append(" (").append(sb).append(")").append(" VALUES(");
    }

    public void appendCommit() throws IOException {
        this.sqlWriter.write("commit;" + lineSeparator);
    }

    @Override
    public void flush() throws IOException {
        this.sqlWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.sqlWriter.close();
    }
}
