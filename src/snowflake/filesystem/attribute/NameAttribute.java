package snowflake.filesystem.attribute;

import java.util.Arrays;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class NameAttribute implements IAttributeValue<String> {
	
	
	/**
	 * <p></p>
	 */
	private final String name;
	
	
	/**
	 * <p></p>
	 */
	private byte[] encoded_name;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public NameAttribute(String name) {
		this.name = Checker.checkForNull(name, GlobalString.Name.toString());
		encoded_name = null;
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public NameAttribute(byte[] buffer) {
		this.name = new String(buffer);
		encoded_name = Arrays.copyOf(buffer, buffer.length);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private byte[] getEncodedName() {
		if( encoded_name == null ) {
			encoded_name = name.getBytes();
		}
		return encoded_name;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData()
	 */
	@Override public byte[] getBinaryData() {
		return Arrays.copyOf(getEncodedName(), encoded_name.length);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		ArrayTool.transferValues(buffer, getEncodedName());
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return getEncodedName().length;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public String getValue() {
		return name;
	}
	
}
