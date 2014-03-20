package cn.edu.sjtu.acm.jdbctaste.entity;

/**
 * Joke is one of our domain model.
 * A joke is = = just a joke.
 * A joke can receive some zan. 
 * Yes, zan is the overall upvote times it received.
 * @author furtherlee
 *
 */
public class Joke {

	private Integer id;

	private final Person speaker;

	private final String body;

	private java.sql.Timestamp postTime;

	private int zan;

	public Joke(Integer id, Person speaker, String body,
			java.sql.Timestamp postTime, int zan) {
		this.id = id;
		this.speaker = speaker;
		this.body = body;
		this.postTime = postTime;
		this.zan = zan;
	}

	public Joke(Person speaker, String body) {
		this(null, speaker, body, null, 0);
	}

	public Integer getId() {
		return id;
	}

	public Person getSpeaker() {
		return speaker;
	}

	public String getBody() {
		return body;
	}

	public java.sql.Timestamp getPostTime() {
		return postTime;
	}

	/**
	 * Get the total number of being up voted
	 * 
	 * @return the number of zan this joke received
	 */
	public int getZan() {
		return zan;
	}

	/**
	 * Up cause a joke another zan
	 */
	public void up() {
		zan++;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void setPostTime(java.sql.Timestamp postTime) {
		this.postTime = postTime;
	}
}
