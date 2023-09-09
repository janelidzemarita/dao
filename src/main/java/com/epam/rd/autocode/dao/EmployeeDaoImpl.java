package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmployeeDaoImpl implements EmployeeDao {

    private static final String ID = "id";
    private static final String LAST_NAME = "lastName";
    private static final String FIRST_NAME = "firstName";
    private static final String MIDDLE_NAME = "middleName";
    private static final String POSITION = "position";
    private static final String SALARY = "salary";
    private static final String HIREDATE = "hiredate";
    private static final String MANAGER = "manager";
    private static final String DEPARTMENT = "department";

    private static final String GET_BY_ID = "SELECT * FROM EMPLOYEE WHERE ID = ?";
    private static final String GET_ALL = "SELECT * FROM EMPLOYEE";
    private static final String GET_ALL_BY_DEP = "SELECT * FROM EMPLOYEE WHERE DEPARTMENT = ?";
    private static final String GET_ALL_BY_MANAGER = "SELECT * FROM EMPLOYEE WHERE MANAGER = ?";
    private static final String INSERT = "INSERT INTO EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, " +
            "POSITION, MANAGER, HIREDATE, SALARY, DEPARTMENT) " +
            "VALUES (?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE = "UPDATE EMPLOYEE SET FIRSTNAME =? , LASTNAME =? , MIDDLENAME =?, " +
            "POSITION =?, MANAGER =?, HIREDATE =?, SALARY =?, DEPARTMENT =? WHERE ID = ?";
    private static final String DELETE = "DELETE FROM EMPLOYEE WHERE ID = ?";

    private static ConnectionSource connectionSource;

    public EmployeeDaoImpl() {
        connectionSource = ConnectionSource.instance();
    }

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        Optional<Employee> employee = Optional.empty();

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_BY_ID)) {

            statement.setString(1, Id.toString());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                employee = Optional.of(new Employee(
                        getId(resultSet),
                        getFullName(resultSet),
                        getPosition(resultSet),
                        getHireDate(resultSet),
                        getSalary(resultSet),
                        getManager(resultSet),
                        getDepartment(resultSet)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }

    private BigInteger getId(ResultSet resultSet) throws SQLException {
        return new BigInteger(resultSet.getString(ID));
    }

    private FullName getFullName(ResultSet resultSet) throws SQLException {
        return new FullName((resultSet.getString(FIRST_NAME)), (resultSet.getString(LAST_NAME)),
                (resultSet.getString(MIDDLE_NAME)));
    }

    private Position getPosition(ResultSet resultSet) throws SQLException {
        return Position.valueOf(resultSet.getString(POSITION));
    }

    private LocalDate getHireDate(ResultSet resultSet) throws SQLException {
        return resultSet.getDate(HIREDATE).toLocalDate();
    }

    private BigDecimal getSalary(ResultSet resultSet) throws SQLException {
        return new BigDecimal(resultSet.getInt(SALARY));
    }

    private BigInteger getManager(ResultSet resultSet) throws SQLException {
        if (resultSet.getString(MANAGER) == null) {
            return new BigInteger("0");
        } else {
            return new BigInteger(resultSet.getString(MANAGER));
        }
    }

    private BigInteger getDepartment(ResultSet resultSet) throws SQLException {
        if (resultSet.getString(DEPARTMENT) == null) {
            return new BigInteger("0");
        } else {
            return new BigInteger(resultSet.getString(DEPARTMENT));
        }
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> list = new ArrayList<>();
        Employee employee;

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL)) {

            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                employee = (new Employee(
                        getId(resultSet),
                        getFullName(resultSet),
                        getPosition(resultSet),
                        getHireDate(resultSet),
                        getSalary(resultSet),
                        getManager(resultSet),
                        getDepartment(resultSet)));
                list.add(employee);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    @Override
    public Employee save(Employee employee) {
        if (getById(employee.getId()).isPresent()) { //Check if employee exists in DB
            update(employee);
        } else {
            insert(employee);
        }
        return employee;
    }

    /**
     * Updates an existing employee's information in the database.

     * This method takes an Employee object with updated information and updates the corresponding
     * record in the database using an SQL UPDATE statement.
     *
     * @param employee The Employee object containing the updated information to be saved to the database.
     * @throws RuntimeException If a database-related SQLException occurs during the update operation,
     *                          it is caught and re-thrown as a RuntimeException.
     */
    private void update(Employee employee) {

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE)) {

            statement.setString(1, employee.getFullName().getFirstName());
            statement.setString(2, employee.getFullName().getLastName());
            statement.setString(3, employee.getFullName().getMiddleName());
            statement.setString(4, employee.getPosition().toString());
            statement.setInt(5, employee.getManagerId().intValue());
            statement.setString(6, employee.getHired().toString());
            statement.setInt(7, employee.getSalary().intValue());
            statement.setInt(8, employee.getDepartmentId().intValue());
            statement.setInt(9, employee.getId().intValue());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insert(Employee employee) {

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {

            statement.setInt(1, employee.getId().intValue());
            statement.setString(2, employee.getFullName().getFirstName());
            statement.setString(3, employee.getFullName().getLastName());
            statement.setString(4, employee.getFullName().getMiddleName());
            statement.setString(5, employee.getPosition().toString());
            statement.setInt(6, employee.getManagerId().intValue());
            statement.setString(7, employee.getHired().toString());
            statement.setInt(8, employee.getSalary().intValue());
            statement.setInt(9, employee.getDepartmentId().intValue());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Employee employee) {

        try (Connection connection = connectionSource.createConnection()) {
            PreparedStatement statement = connection.prepareStatement(DELETE);
            statement.setInt(1, employee.getId().intValue());
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> list = new ArrayList<>();

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL_BY_DEP)) {

            statement.setInt(1, department.getId().intValue());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                list.add(new Employee(
                        getId(resultSet),
                        getFullName(resultSet),
                        getPosition(resultSet),
                        getHireDate(resultSet),
                        getSalary(resultSet),
                        getManager(resultSet),
                        getDepartment(resultSet)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        List<Employee> EmployeeList = new ArrayList<>();

        try (Connection connection = connectionSource.createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL_BY_MANAGER)) {

            statement.setInt(1, employee.getId().intValue());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                EmployeeList.add(new Employee(
                        getId(resultSet),
                        getFullName(resultSet),
                        getPosition(resultSet),
                        getHireDate(resultSet),
                        getSalary(resultSet),
                        getManager(resultSet),
                        getDepartment(resultSet)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return EmployeeList;
    }
}