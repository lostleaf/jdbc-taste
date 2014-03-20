package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case is to test whether you use a cascade style to delete a joke
 * Test Methods: JokeDao.findJokeById, JokeDao.deleteJoke
 * @author furtherlee
 *
 */
public class DeleteJokeTest extends CombinedTask{
	
	private class DeleteJokeTask implements TasteTask {

		private DaoFactory factory;

		public DeleteJokeTask(Taste taste) {
			this.factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {
			try {
				Joke joke = factory.getJokeDao().findJokeById(1);
				factory.getJokeDao().deleteJoke(joke);
				assertEqual(factory.getJokeDao().getAllJokes().size(), 29);
				assertEqual(factory.getCommentDao().getAllComments().size(), 870);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public DeleteJokeTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new FillTableTask(taste));
		addTask(new DeleteJokeTask(taste));
		addTask(new DropTablesTask(taste));
	}
	
}
