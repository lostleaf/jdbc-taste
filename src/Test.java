import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Test {

	public static void main(String[] args) {
		final String PERSON_SCHEMA = "CREATE TABLE if not exists person("
				+ "id integer PRIMARY KEY AUTOINCREMENT,"
				+ "name varchar(200) NOT NULL,"
				+ "email varchar(200) NOT NULL)";
		Connection conn = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:db/jdbc-taste.db");
		} catch (Exception e) {
			throw new RuntimeException("sqlite Connection cannot set up!");
		}

		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute("drop table if exists person");
			stmt.execute(PERSON_SCHEMA);
			stmt.execute("insert into person(name,email) values(\"sdf\",\"ewr\")");
			ResultSet rs = stmt.executeQuery("select * from person");
			while (rs.next())
				System.out.println(rs.getInt("id") + " " + rs.getString(2));
			stmt.close();

		} catch (SQLException e1) {
			e1.printStackTrace();
		}

	}

}
