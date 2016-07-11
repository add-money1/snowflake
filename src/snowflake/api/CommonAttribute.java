package snowflake.api;

import java.time.Instant;
import java.util.function.Function;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.filesystem.Attribute;
import snowflake.filesystem.attribute.NameAttribute;
import snowflake.filesystem.attribute.TimeStampAttribute;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public class CommonAttribute<T> {
	
	
	/**
	 * <p></p>
	 */
	public final static CommonAttribute<Instant> Last_Acces_Time_Stamp = new CommonAttribute<>(
		"last_access_time_stamp", (Instant i) -> new TimeStampAttribute(i)
	);
	
	
	/**
	 * <p></p>
	 */
	public final static CommonAttribute<Instant> Last_Modification_Time_Stamp = new CommonAttribute<>(
		"last_modification_time_stamp", (Instant i) -> new TimeStampAttribute(i)
	);
	
	
	/**
	 * <p></p>
	 */
	public final static CommonAttribute<Instant> Creation_Time_Stamp = new CommonAttribute<>(
		"creation_time_stamp", (Instant i) -> new TimeStampAttribute(i)
	);
	
	
	/**
	 * <p></p>
	 */
	public final static CommonAttribute<String> Name = new CommonAttribute<>(
		"name", (String s) -> new NameAttribute(s)
	);
	
	
	/**
	 * <p></p>
	 */
	private final String name;
		
	
	/**
	 * <p></p>
	 */
	private	final Function<T, IAttributeValue<?>> constructor_function;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	private CommonAttribute(String name, Function<T, IAttributeValue<?>> constructor_function) {
		if( StaticMode.TESTING_MODE ) {
			this.name = Checker.checkForEmptyString(name, GlobalString.Name.toString());
			this.constructor_function = Checker.checkForNull(
				constructor_function, GlobalString.ConstructorFunction.toString()
			);
		}
		else {
			this.name = name;
			this.constructor_function = constructor_function;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Attribute createAttribute(T argument) {
		return new Attribute(name, constructor_function.apply(argument));
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return name;
	}
	
}
