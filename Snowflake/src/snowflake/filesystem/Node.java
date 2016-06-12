package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import j3l.util.check.IValidate;
import snowflake.GlobalString;
import snowflake.api.CommonAttribute;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.12_0
 * @author Johannes B. Latzel
 */
public abstract class Node implements IValidate {

	
	/**
	 * <p></p>
	 */
	private final AttributeCache attribute_cache;
	
	
	/**
	 * <p></p>
	 */
	private IDirectory parent_directory;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_deleted;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected Node(IFlake attribute_flake, IDirectory parent_directory) {
		attribute_cache = new AttributeCache(attribute_flake);
		setParentDirectory(parent_directory);
		is_deleted = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final Collection<Attribute> getAttributes() {
		return attribute_cache.getAttributes();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final Attribute getAttribute(String name) {
		return attribute_cache.getAttribute(name);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void setAttribute(Attribute attribute) {
		attribute_cache.setAttribute(attribute);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final IDirectory getParentDirectory() {
		return parent_directory;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void setParentDirectory(IDirectory parent_directory) {
		ArgumentChecker.checkForNull(parent_directory, GlobalString.ParentDirectory.toString());
		if( this.parent_directory != null ) {
			this.parent_directory.removeChildNode(this);
		}
		this.parent_directory = parent_directory;
		this.parent_directory.addChildNode(this);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final synchronized void delete() {
		if( is_deleted ) {
			return;
		}
		is_deleted = true;
		attribute_cache.delete();
		reactToDeletion();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected abstract void reactToDeletion();
	
	
	/*
	 * (non-Javadoc)
	 * @see j3l.util.check.IValidate#isValid()
	 */
	@Override public boolean isValid() {
		return !is_deleted && attribute_cache.isValid();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		ArrayList<Node> node_path_list = new ArrayList<>(10);
		IDirectory parent = getParentDirectory();
		while( parent instanceof Directory ) {
			// cast okay, because Directory is instanceof Node
			node_path_list.add((Node)parent);
			// cast okay, because parent is instanceof Directory
			parent = ((Directory)parent).getParentDirectory();
		}
		StringBuilder builder = new StringBuilder( node_path_list.size() * 15 + 20 );
		builder.append(FileSystem.NODE_SEPERATOR);
		for(int a=node_path_list.size()-1;a>=0;a--) {
			builder.append(node_path_list.get(a).getAttribute(CommonAttribute.Name.toString()).getAttributeValue().getValue());
			builder.append(FileSystem.NODE_SEPERATOR);
		}
		builder.append(getAttribute(CommonAttribute.Name.toString()).getAttributeValue().getValue());
		return builder.toString();
	}
	
	
}
