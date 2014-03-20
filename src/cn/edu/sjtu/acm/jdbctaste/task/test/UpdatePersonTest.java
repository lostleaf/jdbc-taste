package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case tests whether you could correctly update a person.
 * Test methods: PersonDao.findPersonByEmail, PersonDao.updatePerson, PersonDao.getNumOfJoke, PersonDao.findPersonById
 * @author furtherlee
 *
 */
public class UpdatePersonTest extends CombinedTask{

	private class UpdatePersonTask implements TasteTask {

		private DaoFactory factory;

		public UpdatePersonTask(Taste taste) {
			this.factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {
			try {
				Person acm1 = factory.getPersonDao().findPersonByEmail("acm1@sjtu.edu.cn");
				Person newAcm1 = new Person(acm1.getId(), "newAcm1", "newacm1@sjtu.edu.cn");
				factory.getPersonDao().updatePerson(newAcm1);
				
				Person fetchedPerson = factory.getPersonDao().findPersonById(acm1.getId());
				
				assertEqual(fetchedPerson.getName(), "newAcm1");
				assertEqual(fetchedPerson.getEmail(), "newacm1@sjtu.edu.cn");
				assertEqual(factory.getPersonDao().getNumOfJokes(newAcm1), 1);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public UpdatePersonTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new FillTableTask(taste));
		addTask(new UpdatePersonTask(taste));
		addTask(new DropTablesTask(taste));
	}
}
