package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * A test case to test whether you use a cascade style on person deletion
 * Test Methods: PersonDao.findPersonByEmail, PersonDao.deletePerson, JokeDao.getAllJokes, CommentDao.getAllComments
 * @author furtherlee
 *
 */
public class DeletePersonTest extends CombinedTask {

	private class DeletePersonTask implements TasteTask {

		private DaoFactory factory;

		public DeletePersonTask(Taste taste) {
			this.factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {
			try {
				Person acm1 = factory.getPersonDao().findPersonByEmail("acm1@sjtu.edu.cn");
				factory.getPersonDao().deletePerson(acm1);
				assertEqual(factory.getPersonDao().getAllPerson().size(), 29);
				assertEqual(factory.getJokeDao().getAllJokes().size(), 29);
				assertEqual(factory.getCommentDao().getAllComments().size(), 841);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public DeletePersonTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new FillTableTask(taste));
		addTask(new DeletePersonTask(taste));
		addTask(new DropTablesTask(taste));
	}

}
