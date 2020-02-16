package com.wpi.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
		      File myObj = new File("/Users/aryashrinu/Desktop/MyPage.csv");
		      Scanner myReader = new Scanner(myObj);
		      while (myReader.hasNextLine()) {
		        String data = myReader.nextLine();
		        String[] split = data.split(",");
		        
		        if(split[2].equals("WVLCWEFSJKS")) {
		        	System.out.println(data);
		        	
		        }
		        
		        
		      }
		      myReader.close();
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
	}

}
