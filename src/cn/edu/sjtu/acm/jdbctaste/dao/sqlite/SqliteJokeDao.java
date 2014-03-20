package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import cn.edu.sjtu.acm.jdbctaste.dao.CommentDao;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqliteJokeDao implements JokeDao {

    public static final int IDX_ID = 1, IDX_BODY = 2, IDX_SPEAKER = 3,
            IDX_POST_TIME = 4, IDX_ZAN = 5;

    private final Connection conn;

    public SqliteJokeDao(Connection conn) {
        this.conn = conn;
    }

    @Override
    public int insertJoke(Joke joke) {
        int ret = -1;
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format("insert into joke(body, speaker_id, zan) values(\"%s\", %d, %d)",
                    joke.getBody(), joke.getSpeaker().getId(), joke.getZan()));
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                int id = rs.getInt(1);
                joke.setId(id);
                ret = id;
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public boolean deleteJoke(Joke joke) {
        CommentDao commentDao = DaoFactory.getDaoFactory(0).getCommentDao();
        try {
            Statement stmt = conn.createStatement();
            List<Comment> comments = commentDao.findCommentsOfJoke(joke);
            for (Comment c : comments) commentDao.deleteComment(c);

            stmt.executeUpdate("delete from joke where id = " + joke.getId());
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean updateJoke(Joke joke) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format("update joke set body = \"%s\", speaker_id = %d, zan = %d where id = %d",
                    joke.getBody(), joke.getSpeaker().getId(), joke.getZan(), joke.getId()));
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public List<Joke> findJokesOfPerson(Person person) {
        List<Joke> jokes = new ArrayList<Joke>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from joke where speaker_id = " + person.getId());
            while (rs.next())
                jokes.add(new Joke(rs.getInt("id"), person, rs.getString("body"),
                        rs.getTimestamp("post_time"), rs.getInt("zan")));

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return jokes;
    }

    @Override
    public List<Joke> getAllJokes() {
        PersonDao personDao = DaoFactory.getDaoFactory(0).getPersonDao();
        List<Joke> jokes = new ArrayList<Joke>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from joke");
            while (rs.next()) {
                Person person = personDao.findPersonById(rs.getInt("speaker_id"));
                jokes.add(new Joke(rs.getInt("id"), person, rs.getString("body"),
                        rs.getTimestamp("post_time"), rs.getInt("zan")));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return jokes;
    }

    @Override
    public Joke findJokeById(int id) {
        Joke ret = null;

        try {
            PreparedStatement stat = conn
                    .prepareStatement("select * from joke where id = ?;");
            stat.setInt(1, id);
            ResultSet result = stat.executeQuery();
            if (!result.next())
                return null;

            PersonDao personDao = SqliteDaoFactory.getInstance().getPersonDao();
            Person speaker = personDao.findPersonById(result.getInt("speaker_id"));

            ret = new Joke(result.getInt("id"), speaker,
                    result.getString("body"),
                    result.getTimestamp("post_time"), result.getInt("zan"));

            result.close();
            stat.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public List<Joke> findJokesWithZanMoreThan(int zan) {
        PersonDao personDao = DaoFactory.getDaoFactory(0).getPersonDao();
        List<Joke> jokes = new ArrayList<Joke>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from joke where zan > " + zan);
            while (rs.next()) {
                Person person = personDao.findPersonById(rs.getInt("speaker_id"));
                jokes.add(new Joke(rs.getInt("id"), person, rs.getString("body"),
                        rs.getTimestamp("post_time"), rs.getInt("zan")));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return jokes;
    }
}
