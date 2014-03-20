package cn.edu.sjtu.acm.jdbctaste.task.test;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;
import cn.edu.sjtu.acm.jdbctaste.dao.DaoFactory;
import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.task.CreateTablesTask;
import cn.edu.sjtu.acm.jdbctaste.task.DropTablesTask;

/**
 * This test case is to test whether you could correctly update a comment
 * Test Methods: CommentDao.findCommentById, CommentDao.updateComment
 * @author furtherlee
 *
 */
public class UpdateCommentTest extends CombinedTask {

	private class UpdateCommentTask implements TasteTask {

		private DaoFactory factory;

		public UpdateCommentTask(Taste taste) {
			this.factory = taste.getDaoFactory();
		}

		@Override
		public boolean doit() {
			try {
				Joke joke = factory.getJokeDao().findJokeById(1);

				List<Comment> comments = factory.getCommentDao()
						.findCommentsOfJoke(joke);
				assertEqual(comments.size(), 30);

				Comment comment = comments.get(0);
				Comment newComment = new Comment(
						comment.getId(),
						joke,
						comment.getCommentator(),
						"就在刚才，就在刚才！LZ正在上课，老师在骂我们，说，你不要敬酒不吃吃罚酒！ 然后我弱弱的说了一句：敬酒虽好！ 然后我就苦逼了",
						comment.getPostTime());
				factory.getCommentDao().updateComment(newComment);

				Comment fetchedComment = factory.getCommentDao().findCommentById(
						comment.getId());

				assertEqual(fetchedComment.getBody(),
						"就在刚才，就在刚才！LZ正在上课，老师在骂我们，说，你不要敬酒不吃吃罚酒！ 然后我弱弱的说了一句：敬酒虽好！ 然后我就苦逼了");

			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
	}

	public UpdateCommentTest(Taste taste) {
		addTask(new CreateTablesTask(taste));
		addTask(new FillTableTask(taste));
		addTask(new UpdateCommentTask(taste));
		addTask(new DropTablesTask(taste));
	}
}
