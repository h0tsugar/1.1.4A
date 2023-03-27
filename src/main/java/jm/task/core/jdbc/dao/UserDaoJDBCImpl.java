package jm.task.core.jdbc.dao;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl extends Util implements UserDao {
    Connection connection = getConnection();

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        Statement statement;
        String sql = "CREATE TABLE IF NOT EXISTS Users (" +
                "id INT PRIMARY KEY AUTO_INCREMENT," +
        "userName VARCHAR(255) NOT NULL," +
        "lastName VARCHAR(30)," +
                "age INT NOT NULL)";
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.execute(sql);
            connection.commit();
            System.out.println("Database created!");
        } catch (SQLException e) {
            System.err.println("Cannot connect!");
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void dropUsersTable() {
        Statement statement;
        String sql = "DROP TABLE IF EXISTS Users";
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();
            System.out.println("Database deleted!");
        } catch (SQLException e) {
            System.err.println("Error");
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        PreparedStatement statement;
        String sql = "INSERT Users (userName, lastName, age) VALUES (?, ?, ?)";
        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Error");
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void removeUserById(long id) {
        PreparedStatement statement;
        String sql = "DELETE FROM Users WHERE id=?";
        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            statement.setLong(1, id);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Error");
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        String sql = "SELECT * FROM Users";
        Statement statement;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("userName"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                userList.add(user);
                connection.commit();
            }
        } catch (SQLException e) {
            System.err.println("Error");
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return userList;
    }

    public void cleanUsersTable() {
        PreparedStatement statement;
        String sql = "DELETE FROM Users";
        try {
            connection.setAutoCommit(false);
            statement= connection.prepareStatement(sql);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Error");
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
