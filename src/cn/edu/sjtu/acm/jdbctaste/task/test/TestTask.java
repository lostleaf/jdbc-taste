package cn.edu.sjtu.acm.jdbctaste.task.test;

import cn.edu.sjtu.acm.jdbctaste.TasteTask;

public abstract class TestTask implements TasteTask {

	private void throwExp (String msg){
		throw new RuntimeException("Test Failed ! Since " + msg + "\n");
	}
	
	protected void assertEqual (int a, int b) {
		if (a != b)
			throwExp(a + " != " + b);
	}

	protected void assertEqual (String a, String b) {
		if (!a.equals(b))
			throwExp(a + " != " + b);
	}
	
	protected void assertEqual (java.sql.Timestamp a, java.sql.Timestamp b) {
		if (!a.equals(b))
			throwExp(a.toString() + " != " + b.toString());
	}
}
