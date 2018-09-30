package ua.com.denisimusIT.timeTimer.controller;


import java.sql.*;
import java.util.*;

public class PostgresDatabaseManager implements DatabaseManager {

    private Connection connection;
    private final static String NEW_LINE = System.lineSeparator();


    @Override
    public void connectToDatabase(String databaseName, String userName, String password) {

        if (isConnected() == true) {
            connection = null;
        }

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Please add jdbc jar to project.", e);

        }
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/" + databaseName, userName, password); //TODO вынести в поле
        } catch (SQLException e) {
            connection = null;

            throw new RuntimeException(
                    String.format("Cant get connection for model:%s user:%s", databaseName, userName), e);
        }

    }

    @Override
    public boolean isConnected() {
        return connection != null;
    }


    @Override
    public void clearATable(final String tableName) {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("DELETE FROM " + tableName);
        } catch (SQLException e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    public void createATable(final String tableName, String columnsValues) {

        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS  " + tableName +
                    "(" + columnsValues + ")";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }


    @Override
    public List<DataSet> getTableData(String tableName) {
        ResultSet rs;
        ResultSetMetaData rsmd;

//  yse for big data
//        int size = getSize(tableName);
//        List<DataSet> result = new ArrayList<>(size);
//

        List<DataSet> result = new LinkedList<>();


        try (Statement stmt = connection.createStatement()) {
            rs = stmt.executeQuery("SELECT * FROM " + tableName);

            rsmd = rs.getMetaData();
            getColumnName(rs, rsmd, result);
        } catch (SQLException e) {
            new RuntimeException(e);
        }


        return result;
    }


    @Override
    public List<DataSet> getTableColumn(String tableName, final String columnsName) {

        ResultSet rs;
        ResultSetMetaData rsmd;
        ArrayList<DataSet> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            rs = stmt.executeQuery("SELECT " + columnsName + " FROM " + tableName);
            rsmd = rs.getMetaData();
            getColumnName(rs, rsmd, result);

        } catch (SQLException e) {
            new RuntimeException(e);
        }


        return result;


    }


    private void getColumnName(ResultSet rs, ResultSetMetaData rsmd, List<DataSet> result) throws SQLException {
        while (rs.next()) {
            DataSet dataSet = new DataSet();
            result.add(dataSet);
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                dataSet.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
            }
        }
    }


    private int getSize(String tableName) {
        int size = 0;

        ResultSet rsCount;
        try (Statement stmt = connection.createStatement()) {
            rsCount = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rsCount.next();
            size = rsCount.getInt(1);
            rsCount.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        return size;
    }

    @Override
    public Set<String> getTableNames() {
        //TODO добавить выбор нужной схемы
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='public' " +
                    "AND table_type='BASE TABLE'");
            Set<String> tables = new LinkedHashSet<>();
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
            return tables;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public void insertData(String tableName, DataSet input) {
        // берет значения из  DataSet
        // вставлает их в таблицу

        try (Statement stmt = connection.createStatement()) {

            String columnName = getNameFormatted(input, "%s,");
            String values = getValuesFormatted(input, "'%s',");

            String sql = "INSERT INTO " + tableName + "(" + columnName + ")" + "VALUES (" + values + ")";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            new RuntimeException(e);
        }


    }


    @Override
    public void updateTableData(final String tableName, int id, DataSet newValue) {
        //TODO добавить выбор схемы и колонки

        String tableNames = getNameFormatted(newValue, "%s = ?,");

        String sql = "UPDATE public." + tableName + " SET " + tableNames + " WHERE id = ?";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : newValue.getValues()) {
                ps.setObject(index, value);
                index++;
            }
            ps.setObject(index, id);

            ps.executeUpdate();

        } catch (SQLException e) {
            new RuntimeException(e);
        }
    }


    @Override
    public void dropTable(final String tableName) {
        try (Statement stmt = connection.createStatement();) {
            stmt.executeUpdate("DROP TABLE " + tableName + " ");
            System.out.println("Table " + tableName + " deleted in given database...");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    private String getValuesFormatted(DataSet input, String format) {
        String values = "";
        for (Object value : input.getValues()) {
            values += String.format(format, value);
        }
        values = values.substring(0, values.length() - 1);
        return values;
    }

    private String getNameFormatted(DataSet newValue, String format) {
        String string = "";
        for (String name : newValue.getNames()) {
            string += String.format(format, name);
        }
        string = string.substring(0, string.length() - 1);
        return string;
    }


    @Override
    public void createDatabase(final String databaseName) {

        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE DATABASE " + databaseName;
            stmt.executeUpdate(sql);
        } catch (SQLException se) {
            connection = null;
            se.printStackTrace();
        }

    }


    @Override
    public Set<String> getDatabaseNames() {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery("SELECT datname FROM pg_database");
            Set<String> tables = new LinkedHashSet<>();
            while (rs.next()) {
                tables.add(rs.getString("datname"));
            }
            return tables;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void dropDatabase(final String databaseName) {

        try (Statement stmt = connection.createStatement()) {
            String sql = "DROP DATABASE " + databaseName;
            stmt.executeUpdate(sql);
        } catch (SQLException se) {
            connection = null;
            se.printStackTrace();
        }

    }


    @Override
    public void disconnectOfDatabase(String databaseName) {

        try (Statement stmt = connection.createStatement()) {

            String sql = "SELECT pg_terminate_backend(pg_stat_activity.pid)" + NEW_LINE +
                    "FROM pg_stat_activity" + NEW_LINE +
                    "WHERE pg_stat_activity.datname = " + "'" + databaseName + "'" + NEW_LINE +
                    "  AND pid <> pg_backend_pid();" + NEW_LINE;
            stmt.execute(sql);
            connection = null;
        } catch (SQLException e) {
            new RuntimeException(e);

        }


    }

    @Override
    public List<String> currentDatabase() {

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT current_database();");) {

            List<String> databaseName = new LinkedList<>();
            while (rs.next()) {
                databaseName.add(rs.getString("current_database"));
            }
            return databaseName;
        } catch (SQLException e) {
            new RuntimeException(e);

        }
        return null;
    }


    @Override
    public void giveAccessUserToTheDatabase(String databaseName, String userName) {
        try (Statement stmt = connection.createStatement()) {
            String sql = "GRANT ALL ON DATABASE " + databaseName + " TO  " + userName;
            stmt.executeUpdate(sql);
        } catch (SQLException se) {
            connection = null;
            new RuntimeException(se);
        }
    }

    @Override
    public List<String> getTableColumns(String tableName) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.columns WHERE table_schema='public' " +
                     "AND table_name='" + tableName + "'")) {

            List<String> tables = new LinkedList<>();
            while (rs.next()) {
                tables.add(rs.getString("column_name"));
            }
            return tables;
        } catch (SQLException e) {
            e.printStackTrace();
            return new LinkedList<>();
        }
    }


}