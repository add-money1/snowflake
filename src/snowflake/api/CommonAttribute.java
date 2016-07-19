package snowflake.api;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.19_0
 * @author Johannes B. Latzel
 */
public enum CommonAttribute {
	
	
	Name("name"),
	CreationTimeStamp("creation_time_stamp"),
	LastAccessTimeStamp("last_access_time_stamp"),
	LastModificationTimeStamp("last_modification_time_stamp"),
	DeduplicationDescription("deduplication_description"),
	DataDescription("data_description");
	
	
	/**
	 * <p></p>
	 */
	private final String name;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	private CommonAttribute(String name) {
		if( StaticMode.TESTING_MODE ) {
			this.name = Checker.checkForEmptyString(name, GlobalString.Name.toString());
		}
		else {
			this.name = name;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return name;
	}
	
}
