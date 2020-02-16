package com.wpi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import com.wpi.helpers.RandomGenerator;

public class AllFriendsAccessLog {

	public static void main(String args[]) {

		ArrayList<Integer> personIds = new ArrayList<Integer>();
		HashMap<Integer, HashMap<Integer, Integer>> friendsList = new HashMap<Integer, HashMap<Integer, Integer>>();

		try {
			File myObj = new File(args[0]);
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

			FileWriter myWriter = new FileWriter(args[1]);

			HashMap<Integer, String> friendshipDescDetails = new HashMap<Integer, String>();

			RandomGenerator friendshipdescgenerator = new RandomGenerator("ghijklmnotuvwxyz", "FriendshipDesc");

			for (int i = 1; i <= 100; i++) {

				int friendshipDescLength = friendshipdescgenerator.getRandomNumberInRange(20, 50);
				String friendshipDesc = friendshipdescgenerator.generateRandomString(friendshipDescLength);

				while (friendshipDescDetails.containsValue(friendshipDesc)) {
					friendshipDesc = friendshipdescgenerator.generateRandomString(friendshipDescLength);
				}

				friendshipDescDetails.put(i, friendshipDesc);

			}

			HashMap<Integer, String> accessTypeMap = new HashMap<Integer, String>();

			RandomGenerator accessType = new RandomGenerator("abcghijklmnotuvwxyz", "AccessType");

			for (int k = 1; k <= 100; k++) {

				int accessTypeLength = accessType.getRandomNumberInRange(20, 50);
				String accessTypeName = accessType.generateRandomString(accessTypeLength);

				while (accessTypeMap.containsValue(accessTypeName)) {
					accessTypeName = accessType.generateRandomString(accessTypeLength);
				}

				accessTypeMap.put(k, accessTypeName);

			}

			RandomGenerator random = new RandomGenerator();

			int countRows = 0;

			while (countRows < 10000000) {

				for (int personId : personIds) {

					HashMap<Integer, Integer> friends = friendsList.get(personId);

					ArrayList<Integer> friendsData = new ArrayList<Integer>(friends.keySet());

					int accessLogRows = random.getRandomNumberInRange(1, 50);

					for (int l = 0; l < accessLogRows; l++) {

						int index = random.getRandomNumberInRange(0, 99);
						int friendId = friendsData.get(index);

						int accessId = accessType.getRandomNumberInRange(1, 100);
						String accessName = accessTypeMap.get(accessId);

						RandomGenerator accessTimeGenerator = new RandomGenerator();
						int accessTime = accessTimeGenerator.getRandomNumberInRange(1, 1000000);

						myWriter.write(countRows + "," + personId + "," + friendId + "," + accessName + "," + accessTime
								+ "\n");
						countRows++;

						if (countRows == 10000000) {
							break;
						}

					}

					if (countRows == 10000000) {
						break;
					}

				}

			}

			myWriter.close();
			System.out.println("Successfully wrote to the file.");

			FileWriter allFriends = new FileWriter(args[2]);

			int i = 1;

			for (int personId : personIds) {

				HashMap<Integer, Integer> friends = friendsList.get(personId);

				for (int friendId : friends.keySet()) {

					int friendshipDescId = friendshipdescgenerator.getRandomNumberInRange(1, 100);
					String friendshipDesc = friendshipDescDetails.get(friendshipDescId);

					allFriends.write(i + "," + personId + "," + friendId + ","
							+ friendshipdescgenerator.friendshipDate() + "," + friendshipDesc + "\n");
					i++;
				}

			}

			allFriends.close();

		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

}
