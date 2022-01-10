package org.stark.bitcoin;

import java.util.Timer;
import java.util.TimerTask;

public class bitCoinMain {

	
	public static void main(String[] args) {
		Timer t = new Timer();
		TimerTask task = new bitCoinJob();
		t.schedule(task, 0, 10000);
	}
}