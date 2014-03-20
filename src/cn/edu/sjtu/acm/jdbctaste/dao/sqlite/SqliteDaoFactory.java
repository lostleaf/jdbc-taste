package cn.edu.sjtu.acm.jdbctaste.dao.sqlite;

import java.sql.DriverManager;

import cn.edu.sjtu.acm.jdbctaste.dao.CommentDao;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.dao.JokeDao;
import cn.edu.sjtu.acm.jdbctaste.dao.PersonDao;

public class SqliteDaoFactory extends DaoFactory {

	private static SqliteDaoFactory INSTANCE;

	public static DaoFactory getInstance() {
		if (INSTANCE == null)
			INSTANCE = new SqliteDaoFactory();
		return INSTANCE;
	}

	private SqliteDaoFactory() {
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:db/jdbc-taste.db");
		} catch (Exception e) {
            e.printStackTrace();
			throw new RuntimeException("sqlite Connection cannot set up!");
		}
	}

	@Override
	public PersonDao getPersonDao() {
		return new SqlitePersonDao(conn);
	}

	@Override
	public JokeDao getJokeDao() {
		return new SqliteJokeDao(conn);
	}

	@Override
	public CommentDao getCommentDao() {
		return new SqliteCommentDao(conn);
	}
	
}
