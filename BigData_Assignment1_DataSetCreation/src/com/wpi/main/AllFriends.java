package com.wpi.main;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import com.wpi.helpers.RandomGenerator;

public class AllFriends {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ArrayList<Integer>personIds = new ArrayList<Integer>();
		HashMap<Integer, HashMap<Integer, Integer>> friendsList = new HashMap<Integer, HashMap<Integer, Integer>>();

		try {
			File myObj = new File("/Users/aryashrinu/Desktop/MyPage.csv");
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				String[] result = data.split(",");
				personIds.add(Integer.parseInt(result[0]));
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		for (int personId : personIds) {

			for (int i = 0; i < 100; i++) {

				RandomGenerator random = new RandomGenerator();
				int friendId = random.getRandomNumberInRange(1, 200000);

				if (i == 0) {

					HashMap<Integer, Integer> friends = new HashMap<Integer, Integer>();
					friends.put(friendId, 1);
					friendsList.put(personId, friends);
				}

				else {

					while (personId == friendId) {
						friendId = random.getRandomNumberInRange(1, 200000);
					}

					while (friendsList.get(personId).containsKey(friendId)) {
						friendId = random.getRandomNumberInRange(1, 200000);
					}

					HashMap<Integer, Integer> friends = friendsList.get(personId);
					friends.put(friendId, 1);
					friendsList.put(personId, friends);
				}

			}

		}
		try {
		
		FileWriter myWriter = new FileWriter("/Users/aryashrinu/Desktop/AllFriends.csv");
		
		HashMap<Integer, String> friendshipDescDetails = new HashMap<Integer, String>();
		
    	RandomGenerator friendshipdescgenerator = new RandomGenerator("ghijklmnotuvwxyz","FriendshipDesc"); 
    	
    	for(int i=1;i<=100;i++) {
    	
    	int friendshipDescLength = friendshipdescgenerator.getRandomNumberInRange(20, 50);
    	String friendshipDesc = friendshipdescgenerator.generateRandomString(friendshipDescLength);
    	
    	while (friendshipDescDetails.containsValue(friendshipDesc)) {
    		friendshipDesc = friendshipdescgenerator.generateRandomString(friendshipDescLength);
		}

    	friendshipDescDetails.put(i, friendshipDesc);
    	
    	
    	
    	}
    	
    	int i = 1;

		for (int personId : personIds) {

			HashMap<Integer, Integer> friends = friendsList.get(personId);

			for (int friendId : friends.keySet()) {
				
				int friendshipDescId = friendshipdescgenerator.getRandomNumberInRange(1, 100);
				String friendshipDesc = friendshipDescDetails.get(friendshipDescId);

		    	myWriter.write(i+","+personId + "," + friendId+","+friendshipdescgenerator.friendshipDate()+","+friendshipDesc+"\n");
				i++;
			}

		}
		
		myWriter.close();
		System.out.println("Successfully wrote to the file.");
		}
		catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

}
