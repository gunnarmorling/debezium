package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.PostgresType;

public class ColumnMetaData {

    private final String columnName;
    private final PostgresType postgresType;

    public ColumnMetaData(String columnName, PostgresType postgresType) {
        this.columnName = columnName;
        this.postgresType = postgresType;
    }

    public String getColumnName() {
        return columnName;
    }

    public PostgresType getPostgresType() {
        return postgresType;
    }
}
