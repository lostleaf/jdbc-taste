package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import cn.edu.sjtu.acm.jdbctaste.dao.CommentDao;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class SqlitePersonDao implements PersonDao {

    public static final int IDX_ID = 1, IDX_NAME = 2, IDX_EMAIL = 3;

    private final Connection conn;

    public SqlitePersonDao(Connection conn) {
        this.conn = conn;
    }

    @Override
    public int insertPerson(Person person) {

        int ret = -1;

        try {
            PreparedStatement stat = conn.prepareStatement(
                    "insert into person (name, email) values (?,?);",
                    Statement.RETURN_GENERATED_KEYS);
            stat.setString(1, person.getName());
            stat.setString(2, person.getEmail());

            stat.executeUpdate();

            ResultSet rs = stat.getGeneratedKeys();

            if (rs.next()) {
                int id = rs.getInt(1);
                person.setId(id);
                ret = id;
            }

            rs.close();
            stat.close();
        } catch (SQLException e) {
            e.printStackTrace();
            ret = -1;
        }

        return ret;
    }

    @Override
    public boolean deletePerson(Person person) {
        try {
            CommentDao commentDao = DaoFactory.getDaoFactory(0).getCommentDao();
            JokeDao jokeDao = DaoFactory.getDaoFactory(0).getJokeDao();

            List<Comment> comments = commentDao.findCommentsOfPerson(person);
            for (Comment c : comments)
                commentDao.deleteComment(c);

            List<Joke> jokes = jokeDao.findJokesOfPerson(person);
            for (Joke j : jokes)
                jokeDao.deleteJoke(j);

            PreparedStatement stmt = conn.prepareStatement(
                    "delete from person where id = ?",
                    Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, person.getId());
            stmt.executeUpdate();

            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean updatePerson(Person person) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format("update person set name = \"%s\", email = \"%s\" where id = %d"
                    , person.getName(), person.getEmail(), person.getId()));
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public Person findPersonByEmail(String email) {
        Person person = null;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                    .executeQuery("select * from person where email = \"" + email + '"');
            if (rs.next())
                person = new Person(rs.getInt("id"), rs.getString("name"),
                        rs.getString("email"));
            rs.close();
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return person;
    }

    @Override
    public int getNumOfJokes(Person person) {
        int ret = 0;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from joke where speaker_id = " + person.getId());
            if (rs.next())
                ret = rs.getInt(1);

            rs.close();
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public List<Person> getAllPerson() {
        List<Person> ret = new LinkedList<Person>();

        Statement stat;
        try {
            stat = conn.createStatement();

            stat.execute("select * from person;");
            ResultSet result = stat.getResultSet();

            while (result.next()) {
                ret.add(new Person(result.getInt(IDX_ID), result
                        .getString(IDX_NAME), result.getString(IDX_EMAIL)));
            }
            result.close();
            stat.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return ret;
    }

    @Override
    public Person findPersonById(int id) {
        Person person = null;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                    .executeQuery("select * from person where id = " + id);
            if (rs.next())
                person = new Person(rs.getInt("id"), rs.getString("name"), rs.getString("email"));
            rs.close();
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return person;
    }

}
