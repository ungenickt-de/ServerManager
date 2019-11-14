import static org.junit.Assert.*;

import org.junit.Test;

public class NameValidTest {

	@Test
	public void test() {
		assertTrue(validateName("okname"));
		assertTrue(validateName("thisisfine"));
		assertTrue(validateName("thisisfine-too"));
		assertTrue(validateName("thisisfine3-too"));
		assertFalse(validateName("ba"));
		assertFalse(validateName("superlongnameisnotoddddddk"));
		assertFalse(validateName("this#is$"));
		assertFalse(validateName("this31$#@#@"));
	}
	
	public static boolean validateName(String name) {
		if(name.length() < 3){
			return false;
		}
		if(name.length() > 20){
			return false;
		}
		for(char c : name.toCharArray()){
			if(!Character.isLetter(c) && !Character.isDigit(c) && c != '-'){
				return false;
			}
		}
		
		return true;
	}

}
