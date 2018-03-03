package io.debezium.relational.ddl;

import io.debezium.relational.Tables;

public interface DdlParser {

    void parse(String ddlContent, Tables databaseTables);

    void setCurrentDatabase(String databaseName);

    void setCurrentSchema(String schemaName);
}
