package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import cn.edu.sjtu.acm.jdbctaste.dao.CommentDao;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqliteCommentDao implements CommentDao {

    public static final int IDX_ID = 1, IDX_BODY = 2, IDX_JOKE = 3, IDX_COMMENTATOR = 4, IDX_POST_TIME = 5;
    private final Connection conn;

    public SqliteCommentDao(Connection conn) {
        this.conn = conn;
    }

    @Override
    public int insertComment(Comment comment) {
        int ret = -1;
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format("insert into comment(body, commentator_id, joke_id) values(\"%s\", %d, %d)",
                    comment.getBody(), comment.getCommentator().getId(), comment.getJoke().getId()));
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                int id = rs.getInt(1);
                comment.setId(id);
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
    public boolean deleteComment(Comment comment) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("delete from comment where id = " + comment.getId());
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean updateComment(Comment comment) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(String.format(
                    "update comment set body = \"%s\", commentator_id = %d, joke_id = %d where id = %d",
                    comment.getBody(), comment.getCommentator().getId(), comment.getJoke().getId(), comment.getId()));
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public List<Comment> findCommentsOfPerson(Person person) {
        List<Comment> comments = new ArrayList<Comment>();
        JokeDao jokeDao = DaoFactory.getDaoFactory(0).getJokeDao();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from comment where commentator_id = " + person.getId());
            while (rs.next()) {
                Joke joke = jokeDao.findJokeById(rs.getInt("joke_id"));
                comments.add(new Comment(rs.getInt("id"), joke, person, rs.getString("body"),
                        rs.getTimestamp("post_time")));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return comments;
    }

    @Override
    public List<Comment> findCommentsReceived(Person person) {
        List<Comment> comments = new ArrayList<Comment>();
        List<Joke> jokes = DaoFactory.getDaoFactory(0).getJokeDao().findJokesOfPerson(person);
//        System.err.println(jokes.size());
        for (Joke j : jokes) comments.addAll(findCommentsOfJoke(j));
        return comments;
    }

    @Override
    public List<Comment> findCommentsOfJoke(Joke joke) {
        List<Comment> comments = new ArrayList<Comment>();
        PersonDao personDao = DaoFactory.getDaoFactory(0).getPersonDao();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from comment where joke_id = " + joke.getId());
            while (rs.next()) {
                Person person = personDao.findPersonById(rs.getInt("commentator_id"));
                comments.add(new Comment(rs.getInt("id"), joke, person, rs.getString("body"),
                        rs.getTimestamp("post_time")));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return comments;
    }

    @Override
    public List<Comment> getAllComments() {
        List<Comment> comments = new ArrayList<Comment>();
        PersonDao personDao = DaoFactory.getDaoFactory(0).getPersonDao();
        JokeDao jokeDao = DaoFactory.getDaoFactory(0).getJokeDao();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from comment");
            while (rs.next()) {
                Person person = personDao.findPersonById(rs.getInt("commentator_id"));
                Joke joke = jokeDao.findJokeById(rs.getInt("joke_id"));
                comments.add(new Comment(rs.getInt("id"), joke, person, rs.getString("body"),
                        rs.getTimestamp("post_time")));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return comments;
    }

    @Override
    public Comment findCommentById(int id) {
        Comment comment = null;
        PersonDao personDao = DaoFactory.getDaoFactory(0).getPersonDao();
        JokeDao jokeDao = DaoFactory.getDaoFactory(0).getJokeDao();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from comment where id = " + id);
            if (rs.next()) {
                Person person = personDao.findPersonById(rs.getInt("commentator_id"));
                Joke joke = jokeDao.findJokeById(rs.getInt("joke_id"));
                comment = new Comment(rs.getInt("id"), joke, person, rs.getString("body"),
                        rs.getTimestamp("post_time"));
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return comment;
    }

}
