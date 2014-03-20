package cn.edu.sjtu.acm.jdbctaste.entity;

/**
 * Person is one of our domain model.
 * A person can upvote a joke, can tell a joke, or comment on others' jokes.
 * @author furtherlee
 */
public class Person {

	private Integer id;

	private final String name;

	private final String email;

	public Person(Integer id, String name, String email) {
		this.id = id;
		this.name = name;
		this.email = email;
	}
	
	public Person(String name, String email){
		this(null, name, email);
	}

	public String getName() {
		return name;
	}

	public String getEmail() {
		return email;
	}

	public Integer getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}

	public void upvote(Joke joke) {
		joke.up();
	}

	public Joke tell(String body) {
		return new Joke(this, body);
	}

	public Comment comment(Joke joke, String body) {
		return new Comment(joke, this, body);		
	}
}
