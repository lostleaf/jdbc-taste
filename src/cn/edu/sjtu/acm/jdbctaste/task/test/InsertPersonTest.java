package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case shows how to insert a person and get it back
 * Test Mothod: PersonDao.insertPerson, PersonDao.getAllPerson
 * @author furtherlee
 *
 */
public class InsertPersonTest extends CombinedTask {

	
	private class InsertPersonTask implements TasteTask {

		DaoFactory factory;
		
		public InsertPersonTask(Taste taste) {
			factory = taste.getDaoFactory();
		}
		
		@Override
		public boolean doit() {
			Person godel = new Person("Kurt Godel", "godel@god.org");
			
			factory.getPersonDao().insertPerson(godel);
			
			Person fetchedPerson = factory.getPersonDao().getAllPerson().get(0);
			try {
				assertEqual(fetchedPerson.getName(), "Kurt Godel");
				assertEqual(fetchedPerson.getEmail(), "godel@god.org");
				assertEqual(fetchedPerson.getId(), godel.getId());
			} catch (RuntimeException e){
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}
	
	public InsertPersonTest (Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new InsertPersonTask(taste));
		addTask(new DropTablesTask(taste));
	}
	
}
