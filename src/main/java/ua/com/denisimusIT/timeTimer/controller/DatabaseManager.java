package ua.com.denisimusIT.timeTimer.controller;

import java.util.List;
import java.util.Set;

public interface DatabaseManager {
    void connectToDatabase(String databaseName, String userName, String password);

    void clearATable(String tableName);

    void createATable(String tableName, String columnsValues);

    List<DataSet> getTableData(String tableName);

    List<DataSet> getTableColumn(String tableName, String columnsName);

    Set<String> getTableNames();

    void insertData(String tableName, DataSet input);

    void updateTableData(String tableName, int id, DataSet newValue);

    void dropTable(String tableName);


    void createDatabase(String databaseName);


     Set<String> getDatabaseNames();


    void dropDatabase(final String databaseName);

    void disconnectOfDatabase(String databaseName);

    List<String> currentDatabase();

    void giveAccessUserToTheDatabase(String databaseName, String userName);

    List<String> getTableColumns(String tableName);

    boolean isConnected();

}
