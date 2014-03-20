package cn.edu.sjtu.acm.jdbctaste.dao;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.entity.Joke;
import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public interface JokeDao {

	/**
	 * Insert a joke into the database, it will ignore its `id` and `postTime` fields. 
	 * After insertion, these two field will be automatically filled.
	 * @param joke
	 * @return its id, -1 if failed
	 */
	public int insertJoke(Joke joke);

	/**
	 * Delete a joke according to its primary key `id`. 
	 * You should use cascade way, in which you have to remove all comments related with this joke.
	 * @param joke
	 * @return whether successfully deleting this joke
	 */
	public boolean deleteJoke(Joke joke);

	/**
	 * Update a joke. 
	 * This joke is located by its id (this is the only condition in your where clause).
	 * @param joke
	 * @return whether successfully updating this joke
	 */
	public boolean updateJoke(Joke joke);

	/**
	 * Find a joke by its id
	 * @param id
	 * @return The joke with the input id number
	 */
	public Joke findJokeById(int id);
	
	/**
	 * Find all the jokes that person ever told
	 * @param person
	 * @return A list contains all the jokes this person ever told
	 */
	public List<Joke> findJokesOfPerson(Person person);
	
	/**
	 * Find all the jokes with a upvoted number more than zan
	 * @param the number of zan
	 * @return A list contains all the satisfied jokes
	 */
	public List<Joke> findJokesWithZanMoreThan(int zan);
	
	/**
	 * Get all jokes in the database
	 * @return A list contains all jokes
	 */
	public List<Joke> getAllJokes();
	
}
