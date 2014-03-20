package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case shows some basic use case Test Methods:
 * PersonDao.insertPerson, JokeDao.insertJoke, CommentDao.insertComment
 * 
 * @author furtherlee
 * 
 */
public class InsertTest extends CombinedTask {

	private class InsertTask implements TasteTask {

		private DaoFactory factory;

		public InsertTask(Taste taste) {
			factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {

			Person xiaoShenYang = new Person("Xiao Shenyang", "sy@ne.cn");
			Person zhaoBenShan = new Person("Zhao Benshan", "bs@ne.cn");

			Joke joke = xiaoShenYang
					.tell("大学时，有一次包夜回宿舍，没带钥匙，回去敲门门开了，然后那人往我床上一躺倒头就睡，哥就火了，没事睡我床干嘛。果断一脚带他踹起来，惊讶的发现这人我不认识，哥默默的退了出去。");

			zhaoBenShan.upvote(joke);

			Comment comment = zhaoBenShan.comment(joke, "顶的好舒服！");

			factory.getPersonDao().insertPerson(xiaoShenYang);
			factory.getPersonDao().insertPerson(zhaoBenShan);
			factory.getJokeDao().insertJoke(joke);
			factory.getCommentDao().insertComment(comment);

			try {
				Comment fetchedComment = factory.getCommentDao()
						.findCommentsOfJoke(joke).get(0);
				assertEqual(
						fetchedComment.getBody(),
						"顶的好舒服！");
				assertEqual(fetchedComment.getCommentator().getName(),
						"Zhao Benshan");
				assertEqual(fetchedComment.getJoke().getSpeaker().getName(),
						"Xiao Shenyang");
				assertEqual(fetchedComment.getJoke().getZan(), 1);
			} catch (RuntimeException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public InsertTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new InsertTask(taste));
		addTask(new DropTablesTask(taste));
	}

}
