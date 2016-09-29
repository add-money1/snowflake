package snowflake.filesystem;

import j3l.util.Checker;
import j3l.util.Nameable;
import snowflake.GlobalString;
import snowflake.api.CommonAttribute;
import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.29_0
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
	public Attribute(CommonAttribute common_attribute, IAttributeValue<?> attribute_value) {
		this(common_attribute.toString(), attribute_value);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Attribute(String name, IAttributeValue<?> attribute_value) {
		this.name = Checker.checkForEmptyString(name, GlobalString.Name.toString());
		this.attribute_value = Checker.checkForNull(
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
	
	
	/**
	 * @return
	 */
	public int getBinarySize() {
		return AttributeHeader.SIZE + name.length() + attribute_value.getClass().getName().length()
				+ attribute_value.getDataLength();
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.Nameable#getName()
	 */
	@Override public String getName() {
		return name;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(100);
		string_builder.append("Attribute [name: ");
		string_builder.append(name);
		string_builder.append(" | value: ");
		string_builder.append(attribute_value.toString());
		string_builder.append(']');
		return string_builder.toString();
	}
	
	
}
