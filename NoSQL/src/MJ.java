import org.codehaus.jackson.map.ObjectMapper;


public class MJ {

	public static void main(String[] arg) throws Exception{
		User user = new User();
		user.setAge(55);
		user.setGender('M');
		user.setHeight(170);
		user.setName("Mike Jack");
		user.setAddress(new Address());
		user.getAddress().setCity("Bangalore");
		user.getAddress().setHouseName("House Name");
		user.getAddress().setHouseNum(5);
		user.getAddress().setPin("555000");
		user.getAddress().setState("Karnataka");
		user.getAddress().setStreet1("M G Road");
		user.getAddress().setStreet2("Near \"Some\" \'Where\'");
		ObjectMapper mapper = new ObjectMapper();
		System.out.println( mapper.writeValueAsString(user));
	}
}
