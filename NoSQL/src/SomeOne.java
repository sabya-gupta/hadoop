import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

import oracle.kv.ConsistencyException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.avro.AvroCatalog;
import oracle.kv.avro.GenericAvroBinding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class SomeOne {

	final static Schema.Parser parser = new Schema.Parser();
	
	
	public static void doSomething(){
		try{
			parser.parse(new File("mySchema.avsc"));
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		Schema userInfo = parser.getTypes().get("my.example.userinfo");
		GenericRecord user =  new GenericData.Record(userInfo);
//		user.put("username", "ABC");
//		user.put("age", 25);
//		user.put("phone", "9988776655");
//		user.put("housenum", "1111");
//		user.put("street", "Some Street");
//		user.put("city", "Bangalore");
//		user.put("state_province", "KA");
//		user.put("country", "IND");
//		user.put("zip", "560001");
		
		String[] hosts = {"10.184.134.86:5001"};
		KVStoreConfig kconfig = new KVStoreConfig("kvstore", "10.184.134.86:5000");
		KVStore kvstore = KVStoreFactory.getStore(kconfig); 
		
		AvroCatalog catalog = kvstore.getAvroCatalog();
		GenericAvroBinding userBinding = catalog.getGenericBinding(userInfo);
		
		Key key = Key.createKey(Arrays.asList("user", "2"));
		
		System.out.println(key.toString());
		
//		kvstore.put(key, userBinding.toValue(user));
		
		ValueVersion vv = kvstore.get(key);
		System.out.println(vv);
		
		if(vv!=null){
			GenericRecord usr = new GenericData.Record(userInfo);
			usr = userBinding.toObject(vv.getValue());
			
			System.out.println(
					usr.get("username")+ "|"+
							usr.get("age")+ "|"+
							usr.get("phone")+ "|"+
							usr.get("housenum")+ "|"+
							usr.get("street")+ "|"+
							usr.get("country")+ "|"
					);
		}
		
		key = Key.createKey(Arrays.asList("user", "1"));
//		key = Key.createKey("user/1");
		System.out.println("With brute force");
		
//		kvstore.put(key, userBinding.toValue(user));
		
		vv = kvstore.get(key);
		
		if(vv!=null){
			GenericRecord usr = new GenericData.Record(userInfo);
			usr = userBinding.toObject(vv.getValue());
			
			System.out.println(
					usr.get("username")+ "|"+
							usr.get("age")+ "|"+
							usr.get("phone")+ "|"+
							usr.get("housenum")+ "|"+
							usr.get("street")+ "|"+
							usr.get("country")+ "|"
					);
		}
		
		key = Key.createKey(Arrays.asList("user/1", "user/2"));
		
		SortedMap<Key, ValueVersion> myRecords = null;
		
		try {
			myRecords = kvstore.multiGet(key, null, null);
			} catch (ConsistencyException ce) {
			// The consistency guarantee was not met
				System.out.println(ce.getMessage());
			}
		
		
		System.out.println("Size = "+myRecords.entrySet().size());
		
		GenericRecord usr = new GenericData.Record(userInfo);
		for (int i=0; i<myRecords.size(); i++) {
			vv = myRecords.get(i);
			Value v = vv.getValue();
			usr = userBinding.toObject(vv.getValue());
			
			System.out.println(
					usr.get("username")+ "|"+
							usr.get("age")+ "|"+
							usr.get("phone")+ "|"+
							usr.get("housenum")+ "|"+
							usr.get("street")+ "|"+
							usr.get("country")+ "|"
					);
			
		}
		 
	}
	
	
	public static void DbRw(){
		String[] hosts = {"10.184.134.86:5001"};
//		String[] hosts = {"ofss220343:5001"};
		KVStoreConfig kconfig = new KVStoreConfig("kvstore", "10.184.134.86:5000");
		KVStore kvstore = KVStoreFactory.getStore(kconfig); 
		
		ArrayList<String> majorComponents = new ArrayList<String>();
		ArrayList<String> minorComponents = new ArrayList<String>();
		
		majorComponents.add("Smith");
		majorComponents.add("Bob");
		
		minorComponents.add("phonenumber");
		
		Key myKey = Key.createKey(majorComponents, minorComponents);
		
		Value myValue = Value.createValue("9902490160".getBytes());
		
//		kvstore.put(myKey, myValue);
		
		
		ValueVersion vv = kvstore.get(myKey);
		Value v = vv.getValue(); 
		String data = new String(v.getValue());
		System.out.println(data);
		
	}
	
	
	public static void main(String[] args){
		doSomething();
		System.out.println("Done!!!!");
	}
	
}
