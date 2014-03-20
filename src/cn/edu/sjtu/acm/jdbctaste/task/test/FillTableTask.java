package cn.edu.sjtu.acm.jdbctaste.task.test;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public class FillTableTask implements TasteTask {

	private DaoFactory factory;

	public FillTableTask(Taste taste) {
		factory = taste.getDaoFactory();
	}

	private void fillPersonTable() {
		for (int i = 1; i <= 30; ++i) {
			factory.getPersonDao().insertPerson(
					new Person("person" + i, "acm" + i + "@sjtu.edu.cn"));
		}
	}

	private void fillJokeTable() {
		for (Person p : factory.getPersonDao().getAllPerson())
			factory.getJokeDao().insertJoke(p.tell(p.getName()));
	}

	private void fillCommentTable() {
		List<Person> persons = factory.getPersonDao().getAllPerson();
		List<Joke> jokes = factory.getJokeDao().getAllJokes();

		for (Person p : persons)
			for (Joke j : jokes)
				factory.getCommentDao().insertComment(
						p.comment(j, "hehe from " + p.getName()));
	}

	@Override
	public boolean doit() {
		try {
			fillPersonTable();
			fillJokeTable();
			fillCommentTable();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
