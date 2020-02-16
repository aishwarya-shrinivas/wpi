package com.wpi.helpers;

import java.security.SecureRandom;
import java.util.GregorianCalendar;
import java.util.Random;

public class RandomGenerator {
	
	private String characters_lower;
	private String characters_upper;
	private String random_string;
	
	
	private static final SecureRandom random = new SecureRandom();
	
	public RandomGenerator() {
		
		
	}
	
	public RandomGenerator(String characters_lower,String type) {
		
		this.characters_lower = characters_lower;
		this.characters_upper = characters_lower.toUpperCase();
		
		if(type.equals("Person")) {
			
			random_string = this.characters_lower + this.characters_upper;
			
		}
		else if(type.equals("Nationality")) {
			
			
			random_string = characters_upper;
			
		}
		else if(type.equals("Hobby")) {
			
			
			random_string = characters_lower;
		}
		
		else if(type.equals("FriendshipDesc")) {
			
			random_string = characters_lower;
		}
		else if(type.equals("AccessType")) {
			
			random_string = characters_upper;
		}
	}
	
	public int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
    
	

    public String generateRandomString(int length) {
    	
    	
    	
  if (length < 1) throw new IllegalArgumentException();

        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {

			
            int rndCharAt = random.nextInt(random_string.length());
            char rndChar = random_string.charAt(rndCharAt);

            

            sb.append(rndChar);

        }

        return sb.toString();
    }
    
    
    public  int randDateBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
    
    
    public String friendshipDate() {
    	
    	GregorianCalendar gc = new GregorianCalendar();

    	
    	int year = randDateBetween(2008,2020);
    	gc.set(gc.YEAR, year);
    	
    	int dayOfYear = randDateBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
    	gc.set(gc.DAY_OF_YEAR, dayOfYear);
    	
    	
    	String friendshipDate = gc.get(gc.YEAR) + "-" + (gc.get(gc.MONTH) + 1) + "-" + gc.get(gc.DAY_OF_MONTH);
    	
    	return friendshipDate;
 
    }


}