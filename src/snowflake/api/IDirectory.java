package snowflake.api;

import java.util.Collection;

import snowflake.filesystem.Node;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.19_0
 * @author Johannes B. Latzel
 */
public interface IDirectory {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChildNode(Node node);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void removeChildNode(Node node);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void clear();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Collection<Node> getChildNodes();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getIdentification();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IDirectory getParentDirectory();
	
}
