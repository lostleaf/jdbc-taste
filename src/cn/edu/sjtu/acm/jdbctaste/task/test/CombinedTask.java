package cn.edu.sjtu.acm.jdbctaste.task.test;

import java.util.LinkedList;
import java.util.List;

import cn.edu.sjtu.acm.jdbctaste.TasteTask;

public class CombinedTask extends TestTask {

	protected List<TasteTask> tasks;

	public CombinedTask() {
		tasks = new LinkedList<TasteTask>();
	}

	protected void addTask(TasteTask task) {
		tasks.add(task);
	}

	@Override
	public boolean doit() {
		boolean ret = true;
		for (TasteTask task : tasks)
			ret &= task.doit();
		return ret;
	}
}
