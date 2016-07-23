package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.Checker;
import j3l.util.IValidate;
import j3l.util.Indexable;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.CommonAttribute;
import snowflake.api.FileSystemException;
import snowflake.api.IAttributeValue;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;
import snowflake.api.ILock;
import snowflake.filesystem.attribute.Name;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.23_0
 * @author Johannes B. Latzel
 */
public abstract class Node implements IValidate, Indexable, ILock {

	
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
	 */
	private final long index;
	
	
	/**
	 * <p></p>
	 */
	private Lock lock;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	protected Node(IFlake attribute_flake, IDirectory parent_directory, long index) {
		attribute_cache = new AttributeCache(attribute_flake);
		setParentDirectory(parent_directory);
		is_deleted = false;
		this.index = Checker.checkForBoundaries(index, 0, Long.MAX_VALUE, GlobalString.Index.toString());
		lock = null;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void checkForLock(Lock lock) {
		if( isLocked() && (lock == null || !isLockedBy(lock)) ) {
			throw new FileSystemException("The node is locked!");
		}
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
	public final boolean hasAttribute(CommonAttribute common_attribute) {
		return hasAttribute(common_attribute.toString());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final boolean hasAttribute(String name) {
		for( Attribute attribute : attribute_cache.getAttributes() ) {
			if( attribute.getName().equals(name) ) {
				return true;
			}
		}
		return false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final Attribute getAttribute(CommonAttribute common_attribute) {
		return getAttribute(common_attribute.toString());
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
		setAttribute(attribute, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void setAttribute(Attribute attribute, Lock lock) {
		checkForLock(lock);
		attribute_cache.setAttribute(attribute);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void removeAttribute(CommonAttribute attribute_name) {
		removeAttribute(attribute_name.toString());
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void removeAttribute(String attribute_name) {
		removeAttribute(attribute_name, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void removeAttribute(CommonAttribute attribute_name, Lock lock) {
		removeAttribute(attribute_name.toString(), lock);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void removeAttribute(String attribute_name, Lock lock) {
		checkForLock(lock);
		attribute_cache.removeAttribute(attribute_name);
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
		setParentDirectory(parent_directory, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final void setParentDirectory(IDirectory parent_directory, Lock lock) {
		Checker.checkForNull(parent_directory, GlobalString.ParentDirectory.toString());
		checkForLock(lock);
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
	public final void delete() {
		delete(null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final synchronized void delete(Lock lock) {
		if( is_deleted ) {
			throw new FileSystemException("The node has already been deleted!");
		}
		checkForLock(lock);
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
	public final long getAttributeFlakeIdentification() {
		return attribute_cache.getAttributeFlakeIdentification();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public final String getName() {
		IAttributeValue<?> attribute_value = getAttribute(CommonAttribute.Name).getAttributeValue();
		if( attribute_value instanceof Name ) {
			return ((Name)attribute_value).getValue();
		}
		throw new FileSystemException("The node has not got a name!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public RootDirectory getRootDirectory() {
		IDirectory current_directory = parent_directory;
		while( !(current_directory instanceof RootDirectory) ) {
			current_directory = current_directory.getParentDirectory();
		}
		// cast is okay, because the above loop guarantees that the current_directory is the root-directory
		return (RootDirectory)current_directory;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FileSystem getFileSystem() {
		return getRootDirectory().getFileSystem();
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
	 * @see snowflake.filesystem.ILock#lock()
	 */
	@Override public Lock lock() {
		if( isLocked() ) {
			throw new FileSystemException("The node is locked!");
		}
		return (lock = new Lock());
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.ILock#unlock(snowflake.filesystem.Lock)
	 */
	@Override public final void unlock(Lock lock) {
		if( !isLockedBy(lock) ) {
			throw new FileSystemException("The provided lock \"" + lock.toString() + "\" can not unlock the node!");
		}
		synchronized( this.lock ) {
			this.lock = null;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.ILock#isLocked()
	 */
	@Override public final boolean isLocked() {
		return parent_directory.isLocked() || lock != null;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.ILock#isLockedBy(snowflake.filesystem.Lock)
	 */
	@Override public boolean isLockedBy(Lock lock) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(lock, GlobalString.Lock.toString());
		}
		if( !isLocked() ) {
			throw new FileSystemException("The node is not locked!");
		}
		synchronized( this.lock ) {
			return this.lock == lock;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see j3l.util.Indexable#getIndex()
	 */
	@Override public final long getIndex() {
		return index;
	}
	
	
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
	@Override public final String toString() {
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
