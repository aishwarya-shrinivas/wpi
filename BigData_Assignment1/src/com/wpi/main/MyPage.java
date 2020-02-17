package com.wpi.main;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import com.wpi.helpers.RandomGenerator;

/* Creates MyPage.csv data-set
 * For ID: We used a simple counter that increments by 1 until it reaches 200,000
 * For Name (Upper and Lower Case): We used a random set and number of characters with the help of helper class named RandomGenerator 
 * For Nationality (Upper Case): We used a random set and number of characters (different set of characters than person) with the help of helper class named RandomGenerator
 * For CountryCode: We used a simple counter that increments by 1 until it reaches 50 
 * For Hobby (Lower Case): We used a random set and number of characters (different set of characters than person and nationality) with the help of helper class named RandomGenerator
 */



public class MyPage {
	// TODO Auto-generated method stub

	public static void main(String[] args) {

		HashMap<Integer, String> countryDetails = new HashMap<Integer, String>();
		HashMap<Integer, String> hobbyDetails = new HashMap<Integer, String>();

		RandomGenerator personGenerator = new RandomGenerator("abcdefghijklmnopqrstuvwxyz", "Person");
		RandomGenerator nationalityGenerator = new RandomGenerator("acefjklmnrstuvw", "Nationality");
		RandomGenerator hobbyGenerator = new RandomGenerator("acefjklmnrstuvw", "Hobby");

		for (int j = 1; j <= 50; j++) {

			int nationalityNameLength = nationalityGenerator.getRandomNumberInRange(10, 20);

			String nationality = nationalityGenerator.generateRandomString(nationalityNameLength);

			while (countryDetails.containsValue(nationality)) {
				nationality = nationalityGenerator.generateRandomString(nationalityNameLength);
			}

			countryDetails.put(j, nationality);

		}

		for (int k = 1; k <= 100; k++) {

			int hobbyNameLength = hobbyGenerator.getRandomNumberInRange(10, 20);

			hobbyGenerator = new RandomGenerator("abcdefnopqrstuvwxyz", "Hobby");
			String hobby = hobbyGenerator.generateRandomString(hobbyNameLength);

			while (hobbyDetails.containsValue(hobby)) {
				hobby = hobbyGenerator.generateRandomString(hobbyNameLength);
			}

			hobbyDetails.put(k, hobby);

		}
		try {
			FileWriter myWriter = new FileWriter(args[0]);

			for (int i = 1; i <= 200000; i++) {
				int personNameLength = personGenerator.getRandomNumberInRange(10, 20);

				String person_name = personGenerator.generateRandomString(personNameLength);

				int countryCode = nationalityGenerator.getRandomNumberInRange(1, 50);
				String nationality = countryDetails.get(countryCode);

				int hobbyCode = hobbyGenerator.getRandomNumberInRange(1, 100);
				String hobby = hobbyDetails.get(hobbyCode);

				myWriter.write(i + "," + person_name + "," + nationality + "," + countryCode + "," + hobby + "\n");

			}
			myWriter.close();
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

}
