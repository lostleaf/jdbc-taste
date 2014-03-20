package cn.edu.sjtu.acm.jdbctaste.dao;

import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.entity.Person;

public interface PersonDao {
	
	/**
	 * Insert a person into the database. It will ignore the id field.
	 * Once created, it will automatically field its id.
	 * @param person
	 * @return the id of that person, or -1 if failed
	 */
	public int insertPerson(Person person);

	/**
	 * Delete a specific person in the database. Only condition on its id field.
	 * Notice: This should perform a cascade way, in which you should delete all jokes and comments related to this person.
	 * @param person
	 * @return whether successfully deleting a person.
	 */
	public boolean deletePerson(Person person);

	/**
	 * Update a person information. Only condition on its id to locate.
	 * Tips: it may fail, since our email is unique.
	 * @param person
	 * @return whether updating a person successfully
	 */
	public boolean updatePerson(Person person);

	/**
	 * Try to get a person according to his email
	 * @param email
	 * @return The person with the input email information
	 */
	public Person findPersonByEmail (String email);

	/**
	 * Try to get a person according to his id
	 * @param id
	 * @return The person with the input id number
	 */
	public Person findPersonById (int id);
	
	/**
	 * Try to get the number of jokes which this person ever told
	 * @param person
	 * @return the number of jokes this person once told
	 */
	public int getNumOfJokes (Person person);
	
	/**
	 * Get All the people
	 * @return A list of person contains all the people
	 */
	public List<Person> getAllPerson ();
}
