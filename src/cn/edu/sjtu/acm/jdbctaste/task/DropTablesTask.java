package cn.edu.sjtu.acm.jdbctaste.task;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;

/**
 * This task tries to drop all the tables in an sqlite database
 * @author furtherlee
 *
 */
public class DropTablesTask implements TasteTask {

	private Taste taste;
	
	public DropTablesTask(Taste taste) {
		this.taste = taste;
	}

	@Override
	public boolean doit() {
		try {
			taste.getDaoFactory().getConn().createStatement().execute("drop table person;");
            taste.getDaoFactory().getConn().createStatement().execute("drop table joke;");
            taste.getDaoFactory().getConn().createStatement().execute("drop table comment;");
        } catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
}
