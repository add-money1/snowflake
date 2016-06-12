package snowflake.filesystem;

import j3l.util.Nameable;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.02_0
 * @author Johannes B. Latzel
 */
public final class Attribute implements Nameable {
	
	
	/**
	 * <p></p>
	 */
	private final String name;
	
	
	/**
	 * <p></p>
	 */
	private final IAttributeValue<?> attribute_value;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Attribute(String name, IAttributeValue<?> attribute_value) {
		this.name = ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		this.attribute_value = ArgumentChecker.checkForNull(
			attribute_value, GlobalString.AttributeValue.toString()
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IAttributeValue<?> getAttributeValue() {
		return attribute_value;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.Nameable#getName()
	 */
	@Override public String getName() {
		return name;
	}
	
	
}
