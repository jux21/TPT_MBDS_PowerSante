package tpt.predictor;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import tpt.predictor.controler.Controler;

public class Launcher {

	public static void main(String[] args) {

		LogManager.getLogger("org").setLevel(Level.OFF);

		new Controler();
	}
}
