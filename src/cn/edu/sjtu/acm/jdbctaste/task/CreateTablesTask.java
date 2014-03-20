package cn.edu.sjtu.acm.jdbctaste.task;

import cn.edu.sjtu.acm.jdbctaste.Taste;
import cn.edu.sjtu.acm.jdbctaste.TasteTask;

/**
 * This task is to create tree tables in sqlite, called person, joke and comment
 * Caution: Don't use reference, we first use naive way to do the same thing.
 * Notice: In sqlite, database used is infereced from your connection url, so no "create database taste;" or "use taste"
 *
 * @author furtherlee
 */
public class CreateTablesTask implements TasteTask {

    private Taste taste;

    public CreateTablesTask(Taste taste) {
        this.taste = taste;
    }
    private static final String PERSON_SCHEMA =
            "CREATE TABLE person(" +
                    "id integer PRIMARY KEY AUTOINCREMENT," +
                    "name varchar(200) NOT NULL," +
                    "email varchar(200) NOT NULL);";

    private static final String JOKE_SCHEMA =
            "CREATE TABLE joke(" +
                    "id integer PRIMARY KEY AUTOINCREMENT," +
                    "body varchar(200) NOT NULL," +
                    "speaker_id integer REFERENCES person(id)," +
                    "post_time timestamp NOT NULL default current_timestamp," +
                    "zan integer NOT NULL);";
    private static final String COMMENT_SCHEMA = "create table comment (" +
            "  id integer primary key AUTOINCREMENT," +
            "  body varchar(200) not null," +
            "  joke_id integer REFERENCES joke(id)," +
            "  commentator_id integer references person(id)," +
            "  post_time timestamp not null default current_timestamp);";

    @Override
    public boolean doit() {
//    	System.out.println(PERSON_SCHEMA);
//    	System.out.println(JOKE_SCHEMA);
        try {
            taste.getDaoFactory().getConn().createStatement().execute(PERSON_SCHEMA);
            taste.getDaoFactory().getConn().createStatement().execute(JOKE_SCHEMA);
            taste.getDaoFactory().getConn().createStatement().execute(COMMENT_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
