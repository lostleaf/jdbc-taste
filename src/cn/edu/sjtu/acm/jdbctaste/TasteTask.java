package cn.edu.sjtu.acm.jdbctaste;

/**
 * This is a taste task interface.
 * A taste task is something taste will do for setting up database, test, or something else.
 * @author furtherlee
 *
 */
public interface TasteTask {
	boolean doit();
}
