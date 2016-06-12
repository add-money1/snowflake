package snowflake.filesystem;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;
import snowflake.api.StorageException;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.11_0
 * @author Johannes B. Latzel
 */
public final class Directory extends Node implements IDirectory {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Node> child_node_list;
	
	
	/**
	 * @param attribute_flake
	 */
	public Directory(IFlake attribute_flake, IDirectory parent_directory) {
		super(attribute_flake, parent_directory);
		child_node_list = new ArrayList<>();
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
	 * @see snowflake.filesystem.Node#reactToDeletion()
	 */
	@Override protected final void reactToDeletion() {
		clear();
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

}
