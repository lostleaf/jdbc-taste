package cn.edu.sjtu.acm.jdbctaste.dao;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.entity.Comment;
import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public interface CommentDao {

	/**
	 * Create a comment in the database. It will ignore the id and postTime fields.
	 * Once created, these two fields will be automatically filled.
	 * @param comment
	 * @return the id of this comment, -1 if failed
	 */
	public int insertComment(Comment comment);

	/**
	 * Remove a comment in the database. Only condition on its id to locate a comment.
	 * @param comment
	 * @return whether deleting the comment successfully
	 */
	public boolean deleteComment(Comment comment);

	/**
	 * Update a comment in the database. Only condition on its id to locate a comment.
	 * @param comment
	 * @return whether updating the comment successfully
	 */
	public boolean updateComment(Comment comment);
	
	/**
	 * Find a comment according to its id
	 * @param id
	 * @return The comment with the input id number
	 */
	public Comment findCommentById(int id);
	
	/**
	 * Get all the comments this person gave to others
	 * @param person
	 * @return A list contains all the comments this person gave to others
	 */
	public List<Comment> findCommentsOfPerson(Person person);

	/**
	 * Get all the comments this person received from others
	 * @param person
	 * @return A list contains all the comments this person received from others
	 */
	public List<Comment> findCommentsReceived(Person person);
	
	/**
	 * Get all the comments of a specific joke
	 * @param joke
	 * @return A list contains all the comments of a specific joke
	 */
	public List<Comment> findCommentsOfJoke(Joke joke);

	/**
	 * Get all comments
	 * @return A list contains all comments
	 */
	public List<Comment> getAllComments();

}
