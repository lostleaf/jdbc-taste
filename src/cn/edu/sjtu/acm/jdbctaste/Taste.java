package cn.edu.sjtu.acm.jdbctaste;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.task.test.DeleteCommentTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.DeleteJokeTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.DeletePersonTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.InsertPersonTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.InsertTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.UpdateCommentTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.UpdateJokeTest;
import cn.edu.sjtu.acm.jdbctaste.task.test.UpdatePersonTest;

/**
 * A taste for JDBC
 * Welcome to the world of Fatworm!
 * @author furtherlee
 */
public class Taste {

	private List<TasteTask> tasks;

	private DaoFactory factory;

	private int passedTests;
	
	public Taste() {
		tasks = new ArrayList<TasteTask>();
		factory = DaoFactory.getDaoFactory(DaoFactory.SQLITE);
	}

	public DaoFactory getDaoFactory() {
		return factory;
	}

	public void addTask(TasteTask task) {
		tasks.add(task);
	}

	public void doTasks() {
		passedTests = 0;
		for (TasteTask task: tasks)
			if (task.doit()) {
				System.out.println(task.getClass().getSimpleName() + " succeed!");
				passedTests++;
			}
			else
				System.out.println(task.getClass().getSimpleName() + " failed!");
	}

	private void deleteDb() {
		File file = new File("db" + File.separator + "jdbc-taste.db");
		if (file.exists())
			file.deleteOnExit();
	}
	
	public void finish() {
		System.out.println("You passed " + passedTests + "/" + tasks.size() + " tests.");
		factory.closeConn();
		deleteDb();
	}
	
	public static void main(String[] args) {
		Taste taste = new Taste();
		taste.addTask(new InsertPersonTest(taste));
		taste.addTask(new DeletePersonTest(taste));
		taste.addTask(new UpdatePersonTest(taste));
		taste.addTask(new InsertTest(taste));
		taste.addTask(new DeleteJokeTest(taste));
		taste.addTask(new UpdateJokeTest(taste));
		taste.addTask(new DeleteCommentTest(taste));
		taste.addTask(new UpdateCommentTest(taste));
		taste.doTasks();
		taste.finish();
	}
}
