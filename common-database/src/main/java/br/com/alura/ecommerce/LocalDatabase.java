package br.com.alura.ecommerce;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void update(String statement, String... params) throws SQLException {
        PreparedStatement insert = prepare(statement, params);
        insert.execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        PreparedStatement stmt = prepare(query, params);
        return stmt.executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        PreparedStatement insert = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            insert.setString(i + 1, params[i]);
        }
        return insert;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
