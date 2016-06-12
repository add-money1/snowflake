package snowflake.filesystem.attribute;

import j3l.util.ArrayTool;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.10_0
 * @author Johannes B. Latzel
 */
public final class NameAttribute implements IAttributeValue<String> {
	
	
	/**
	 * <p></p>
	 */
	private final String name;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public NameAttribute(String name) {
		this.name = ArgumentChecker.checkForNull(name, GlobalString.Name.toString());
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public NameAttribute(byte[] buffer) {
		this.name = new String(buffer);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData()
	 */
	@Override public byte[] getBinaryData() {
		return name.getBytes();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		ArrayTool.transferValues(buffer, name.getBytes());
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return name.length() * 2;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public String getValue() {
		return name;
	}
	
}
