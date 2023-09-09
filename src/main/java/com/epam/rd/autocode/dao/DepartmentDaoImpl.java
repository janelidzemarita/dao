package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDaoImpl implements DepartmentDao {

    private static final String ID = "ID";
    private static final String NAME = "NAME";
    private static final String LOCATION = "LOCATION";

    private static final String INSERT = "INSERT INTO DEPARTMENT (ID, NAME, LOCATION) VALUES (?,?,?)";
    private static final String UPDATE = "UPDATE DEPARTMENT SET NAME = ?, LOCATION = ? WHERE ID = ?";
    private static final String DELETE = "DELETE FROM DEPARTMENT WHERE ID = ?";
    private static final String GET_BY_ID = "SELECT * FROM DEPARTMENT WHERE ID = ?";
    private static final String GET_ALL = "SELECT * FROM DEPARTMENT";

    private static ConnectionSource connectionSource;

    public DepartmentDaoImpl() {
        connectionSource = ConnectionSource.instance();
    }

    @Override
    public Optional<Department> getById(BigInteger Id) {
        Optional<Department> department = Optional.empty();

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_BY_ID)) {

            statement.setString(1, Id.toString());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                department = Optional.of(new Department(new BigInteger(resultSet.getString(ID)),
                        resultSet.getString(NAME),
                        resultSet.getString(LOCATION)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return department;
    }

    @Override
    public List<Department> getAll() {
        List<Department> departmentList = new ArrayList<>();

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL);
             ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                departmentList.add(new Department(new BigInteger(resultSet.getString(ID)),
                        resultSet.getString(NAME),
                        resultSet.getString(LOCATION)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return departmentList;
    }

    @Override
    public Department save(Department department) {
        if (getById(department.getId()).isPresent()) { // if department exists
            update(department);
        } else {
            insert(department);
        }
        return department;
    }

    private void update(Department department) {

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE)) {

            statement.setString(1, department.getName());
            statement.setString(2, department.getLocation());
            statement.setInt(3, department.getId().intValue());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insert(Department department) {

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {

            statement.setInt(1, department.getId().intValue());
            statement.setString(2, department.getName());
            statement.setString(3, department.getLocation());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Department department) {

        try (Connection connection = connectionSource.createConnection()) {

            PreparedStatement statement = connection.prepareStatement(DELETE);
            statement.setInt(1, department.getId().intValue());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}