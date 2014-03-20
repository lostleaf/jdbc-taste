package cn.edu.sjtu.acm.jdbctaste.task.test;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case is to test you whether you correctly delete a comment
 * Test Methods: CommentDao.deleteComment, CommentDao.findCommentsOfPerson, CommentDao.findCommentsReceived
 * @author furtherlee
 */
public class DeleteCommentTest extends CombinedTask {
	
	private class DeleteCommentTask implements TasteTask {

		private DaoFactory factory;

		public DeleteCommentTask(Taste taste) {
			this.factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {
			try {
				Person person = factory.getPersonDao().findPersonById(1);
				
				List<Comment> hisComments = factory.getCommentDao().findCommentsOfPerson(person);
				assertEqual(hisComments.size(), 30);
				
				List<Comment> receivedComments = factory.getCommentDao().findCommentsReceived(person);
				assertEqual(receivedComments.size(), 30);
				
				Comment selfComment = null;
				for(Comment c: hisComments)
					if (c.getCommentator().getId().equals(person.getId())) {
						selfComment = c;
						break;
					}
				
				factory.getCommentDao().deleteComment(selfComment);
				assertEqual(factory.getCommentDao().findCommentsOfPerson(person).size(), 29);
				assertEqual(factory.getCommentDao().findCommentsReceived(person).size(), 29);
			
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public DeleteCommentTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new FillTableTask(taste));
		addTask(new DeleteCommentTask(taste));
		addTask(new DropTablesTask(taste));
	}
}
