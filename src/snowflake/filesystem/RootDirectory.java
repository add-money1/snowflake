package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.FileSystemException;
import snowflake.api.IDirectory;
import snowflake.api.StorageException;
import snowflake.core.manager.FlakeManager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.19_0
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
	 * @param attribute_flake
	 */
	public RootDirectory(FileSystem file_system) {
		this.file_system = ArgumentChecker.checkForNull(file_system, GlobalString.FileSystem.toString());
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
		ArgumentChecker.checkForValidation(node, GlobalString.Node.toString());
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
		ArgumentChecker.checkForValidation(node, GlobalString.Node.toString());
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
	
}
