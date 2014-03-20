package cn.edu.sjtu.acm.jdbctaste.entity;

/**
 * Comment is another domain model.
 * A comment is what someone said about a joke.
 * @author furtherlee
 */
public class Comment {

	private Integer id;

	private final Joke joke;

	private final Person commentator;

	private final String body;

	private java.sql.Timestamp postTime;

	public Comment(Integer id, Joke joke, Person commentator, String body,
			java.sql.Timestamp postTime) {
		this.id = id;
		this.joke = joke;
		this.commentator = commentator;
		this.body = body;
		this.postTime = postTime;
	}

	public Comment(Joke joke, Person commentator, String body) {
		this(null, joke, commentator, body, null);
	}

	public Integer getId() {
		return id;
	}

	public Joke getJoke() {
		return joke;
	}

	public Person getCommentator() {
		return commentator;
	}

	public String getBody() {
		return body;
	}

	public java.sql.Timestamp getPostTime() {
		return postTime;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
	public void setPostTime(java.sql.Timestamp postTime) {
		this.postTime = postTime;
	}
}
