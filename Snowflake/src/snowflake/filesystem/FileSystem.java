package snowflake.filesystem;

import java.time.Instant;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.CommonAttribute;
import snowflake.api.FileSystemException;
import snowflake.api.IAttributeValue;
import snowflake.api.IDirectory;
import snowflake.core.storage.Storage;
import snowflake.filesystem.attribute.NameAttribute;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.12_0
 * @author Johannes B. Latzel
 */
public class FileSystem {
	
	
	/**
	 * <p></p>
	 */
	public final static String NODE_SEPERATOR = "/";
	
	
	/**
	 * <p></p>
	 */
	private final Storage storage;
	
	
	/**
	 * <p></p>
	 */
	private final RootDirectory root_directory;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public FileSystem(Storage storage) {
		this.storage = ArgumentChecker.checkForNull(storage, GlobalString.Storage.toString());
		root_directory = new RootDirectory();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public File createFile(String name) {
		return createFile(name, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public File createFile(String name, IDirectory parent_directory) {
		File file = new File(
			storage.createFlake(), storage.createFlake(), parent_directory != null ? parent_directory : root_directory
		);
		Instant now = Instant.now();
		file.setAttribute(CommonAttribute.Creation_Time_Stamp.createAttribute(now));
		file.setAttribute(CommonAttribute.Last_Acces_Time_Stamp.createAttribute(now));
		file.setAttribute(CommonAttribute.Last_Modification_Time_Stamp.createAttribute(now));
		file.setAttribute(
			CommonAttribute.Name.createAttribute(
				ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString())
			)
		);
		return file;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IDirectory createDirectory(String name) {
		return createDirectory(name, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IDirectory createDirectory(String name, IDirectory parent_directory) {
		Directory directory = new Directory(
			storage.createFlake(), parent_directory != null ? parent_directory : root_directory
		);
		Instant now = Instant.now();
		directory.setAttribute(CommonAttribute.Creation_Time_Stamp.createAttribute(now));
		directory.setAttribute(CommonAttribute.Last_Acces_Time_Stamp.createAttribute(now));
		directory.setAttribute(CommonAttribute.Last_Modification_Time_Stamp.createAttribute(now));
		directory.setAttribute(
			CommonAttribute.Name.createAttribute(
				ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString())
			)
		);
		return directory;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Node getNode(String path) {
		String[] path_elements = ArgumentChecker.checkForEmptyString(
			path, GlobalString.Path.toString()).split(FileSystem.NODE_SEPERATOR
		);
		if( path_elements == null || path_elements.length == 0 ) {
			throw new FileSystemException("The root is not a node!");
		}
		if( path_elements[0].length() != 0 ) {
			throw new FileSystemException("The path must begin with: \"" + FileSystem.NODE_SEPERATOR + "\"!");
		}
		IDirectory current_directory = root_directory;
		Collection<Node> child_node_collection;
		IAttributeValue<?> current_attribute_value;
		boolean found_path_element;
		for(int a=1,n=path_elements.length-1;a<n;a++) {
			child_node_collection = current_directory.getChildNodes();
			found_path_element = false;
			for( Node node : child_node_collection ) {
				if( node instanceof IDirectory ) {
					current_attribute_value = node.getAttribute(CommonAttribute.Name.toString()).getAttributeValue();
					if( current_attribute_value instanceof NameAttribute 
						&& ((NameAttribute)current_attribute_value).getValue().equals(path_elements[a]) ) {
						current_directory = (IDirectory)node;
						found_path_element = true;
						break;
					}
				}
			}
			if( !found_path_element ) {
				throw new FileSystemException("Can not resolve the path \"" + path + "\" - Uknown path_element \""
						+ path_elements[a] + "\"!");
			}
		}
		child_node_collection = current_directory.getChildNodes();
		for( Node node : child_node_collection ) {
			current_attribute_value = node.getAttribute(CommonAttribute.Name.toString()).getAttributeValue();
			if( current_attribute_value instanceof NameAttribute 
				&& ((NameAttribute)current_attribute_value).getValue().equals(
						path_elements[path_elements.length - 1]) ) {
				return node;
			}
		}
		throw new FileSystemException("Can not resolve the path \"" + path + "\" - Uknown path_element \""
				+ path_elements[path_elements.length - 1] + "\"!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public RootDirectory getRootDirectory() {
		return root_directory;
	}
	
}
