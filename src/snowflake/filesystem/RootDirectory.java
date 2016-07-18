package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.FileSystemException;
import snowflake.api.IDirectory;
import snowflake.api.StorageException;
import snowflake.core.manager.FlakeManager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.18_0
 * @author Johannes B. Latzel
 */
public class RootDirectory implements IDirectory {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Node> child_node_list;
	
	
	/**
	 * <p></p>
	 */
	private final FileSystem file_system;
	
	
	/**
	 * <p></p>
	 */
	private Lock lock;
	
	
	/**
	 * @param attribute_flake
	 */
	public RootDirectory(FileSystem file_system) {
		this.file_system = Checker.checkForNull(file_system, GlobalString.FileSystem.toString());
		child_node_list = new ArrayList<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FileSystem getFileSystem() {
		return file_system;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IDirectory#removeChildNode(snowflake.filesystem.Node)
	 */
	@Override public void addChildNode(Node node) {
		Checker.checkForValidation(node, GlobalString.Node.toString());
		synchronized( child_node_list ) {
			if( child_node_list.contains(node) ) {
				throw new StorageException("The node \"" + node.toString() + "\" is already a child-node!");
			}
			if( !child_node_list.add(node) ) {
				throw new StorageException("The node \"" + node.toString() + "\" could not be added!");
			}
		}
		if( node.getParentDirectory() != this ) {
			node.setParentDirectory(this);
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IDirectory#removeChildNode(snowflake.filesystem.Node)
	 */
	@Override public void removeChildNode(Node node) {
		Checker.checkForValidation(node, GlobalString.Node.toString());
		synchronized( child_node_list ) {
			if( !child_node_list.contains(node) ) {
				throw new StorageException("The directory does not contain the node \"" + node.toString() + "\"!");
			}
			if( !child_node_list.remove(node) ) {
				throw new StorageException("The node \"" + node.toString() + "\" could not be removed!");
			}
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IDirectory#removeChildNode(snowflake.filesystem.Node)
	 */
	@Override public void clear() {
		synchronized( child_node_list ) {
			child_node_list.forEach(Node::delete);
			child_node_list.clear();
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IDirectory#getChildNodes()
	 */
	@Override public Collection<Node> getChildNodes() {
		synchronized( child_node_list ) {
			return new ArrayList<>(child_node_list);
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return FileSystem.NODE_SEPERATOR;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IDirectory#getIdentification()
	 */
	@Override public long getIdentification() {
		return FlakeManager.ROOT_IDENTIFICATION;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IDirectory#getParentDirectory()
	 */
	@Override public IDirectory getParentDirectory() {
		throw new FileSystemException("The root-directory has not got a parent-directory!");
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.ILock#lock()
	 */
	@Override public Lock lock() {
		if( isLocked() ) {
			throw new FileSystemException("The root_directory is already locked!");
		}
		return (lock = new Lock());
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.ILock#unlock(snowflake.filesystem.Lock)
	 */
	@Override public final void unlock(Lock lock) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(lock, GlobalString.Lock.toString());
		}
		if( !isLocked() ) {
			throw new FileSystemException("The root_directory is not locked!");
		}
		synchronized( lock ) {
			if( this.lock != lock ) {
				throw new FileSystemException(
					"The provided lock \"" + lock.toString() + "\" can not unlock the root_directory!"
				);
			}
			this.lock = null;
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.ILock#isLocked()
	 */
	@Override public final boolean isLocked() {
		return lock != null;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.ILock#isLockedBy(snowflake.filesystem.Lock)
	 */
	@Override public boolean isLockedBy(Lock lock) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(lock, GlobalString.Lock.toString());
		}
		if( !isLocked() ) {
			throw new FileSystemException("The root_directory is not locked!");
		}
		synchronized( this.lock ) {
			return this.lock == lock;
		}
	}
	
}
