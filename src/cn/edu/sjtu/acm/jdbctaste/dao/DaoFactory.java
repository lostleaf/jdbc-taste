package cn.edu.sjtu.acm.jdbctaste.dao;

import cn.edu.sjtu.acm.jdbctaste.dao.sqlite.*;

import java.sql.Connection;

/**
 * A DAO Factory. Since no concurrency here, we implement the simplest singleton.
 * It is responsible to build other DAO as a factory.
 * @author furtherlee
 *
 */
public abstract class DaoFactory {

	protected Connection conn;
	
	public static final int SQLITE = 0;

	public abstract PersonDao getPersonDao();

	public abstract JokeDao getJokeDao();

	public abstract CommentDao getCommentDao();

	public Connection getConn() {
		return conn;
	}
	
	public boolean closeConn() {
		try {
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public static DaoFactory getDaoFactory(int type) {
		switch (type) {
		case SQLITE:
			return SqliteDaoFactory.getInstance();
		default:
			return null;
		}
	}
}
