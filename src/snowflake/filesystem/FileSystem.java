package snowflake.filesystem;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.CommonAttribute;
import snowflake.api.FileSystemException;
import snowflake.api.IDirectory;
import snowflake.api.IFlake;
import snowflake.core.storage.Storage;
import snowflake.filesystem.manager.DeduplicationManager;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.09_0
 * @author Johannes B. Latzel
 */
public class FileSystem implements IClose<FileSystemException> {
	
	
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
	 */
	private final FileTable file_table;
	
	
	/**
	 * <p></p>
	 */
	private final DirectoryTable directory_table;
	
	
	/**
	 * <p></p>
	 */
	private final DeduplicationManager deduplication_manager;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
		
	
	/**
	 * <p></p>
	 * 
	 * @param
	 * @throws IOException 
	 */
	public FileSystem(Storage storage) {
		this.storage = ArgumentChecker.checkForNull(storage, GlobalString.Storage.toString());
		root_directory = new RootDirectory(this);
		try {
			file_table = new FileTable(storage.getFileTableFlake());
		}
		catch( IOException e ) {
			throw new FileSystemException("Can create the file_table!", e);
		}
		try {
			directory_table = new DirectoryTable(storage.getDirectoryTableFlake());
		}
		catch( IOException e ) {
			throw new FileSystemException("Can create the directory_table!", e);
		}
		deduplication_manager = new DeduplicationManager(this, storage.getDeduplicationTableFlake());
		closure_state = ClosureState.None;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	private void load() throws IOException {
		ArrayList<DirectoryData> directory_data_list = directory_table.getAllEntries();
		ArrayList<FileData> file_data_list = file_table.getAllEntries();
		Map<Long, IDirectory> map = new HashMap<>();
		map.put(new Long(0), root_directory);
		Long current_parent_indentification;
		IDirectory current_directory;
		DirectoryData current_directory_data;
		// this may cause an infinite loop (if some directories' parents do not exist)..
		// and it may be slow since at every iteration a new Long is created..
		while( !directory_data_list.isEmpty() ) {
			for(int a=directory_data_list.size()-1;a>=0;a--) {
				current_directory_data = directory_data_list.get(a);
				current_parent_indentification = new Long(current_directory_data.getParentDirectoryIdentification());
				if( !map.containsKey(current_parent_indentification) ) {
					continue;
				}
				current_directory = new Directory(
					storage.getFlake(current_directory_data.getAttributeFlakeIdentification()),
					map.get(current_parent_indentification),
					current_directory_data.getIndex()
				);
				directory_data_list.remove(a);
				map.put(new Long(current_directory.getIdentification()), current_directory);
			}
		}
		IFlake current_data_flake;
		boolean needs_to_be_saved = false;
		File current_file;
		for( FileData file_data : file_data_list ) {
			current_parent_indentification = new Long(file_data.getParentDirectoryIdentification());
			if( !map.containsKey(current_parent_indentification) ) {
				throw new FileSystemException(
					"The parent directory of " + file_data.toString()
					+ "does not exist!"
				);
			}
			current_directory = map.get(current_parent_indentification);
			current_data_flake = storage.getFlake(file_data.getDataFlakeIdentification());
			if( current_data_flake == null ) {
				current_data_flake = storage.createFlake();
				needs_to_be_saved = true;
			}
			current_file = new File(
				storage.getFlake(file_data.getAttributeFlakeIdentification()),
				current_data_flake,
				current_directory,
				file_data.getIndex()
			);
			if( needs_to_be_saved ) {
				file_table.saveEntry(current_file);
				needs_to_be_saved = false;
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public File createFile(String name) throws IOException {
		return createFile(name, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public File createFile(String name, IDirectory parent_directory) throws IOException {
		File file = new File(
			storage.createFlake(),
			storage.createFlake(),
			parent_directory != null ? parent_directory : root_directory,
			file_table.getAvailableIndex()
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
		file_table.saveEntry(file);
		return file;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public IDirectory createDirectory(String name) throws IOException {
		return createDirectory(name, null);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public IDirectory createDirectory(String name, IDirectory parent_directory) throws IOException {
		Directory directory = new Directory(
			storage.createFlake(),
			parent_directory != null ? parent_directory : root_directory,
			directory_table.getAvailableIndex()
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
		directory_table.saveEntry(directory);
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
		boolean found_path_element;
		for(int a=1,n=path_elements.length-1;a<n;a++) {
			child_node_collection = current_directory.getChildNodes();
			found_path_element = false;
			for( Node node : child_node_collection ) {
				if( node instanceof IDirectory ) {
					if( node.getName().equals(path_elements[a]) ) {
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
			if( node.getName().equals(path_elements[path_elements.length - 1]) ) {
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
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IClose#open()
	 */
	@Override public synchronized void open() {
		if( isOpen() ) {
			if( StaticMode.TESTING_MODE ) {
				throw new FileSystemException("The FileSystem has already been opened!");
			}
			return;
		}
		closure_state = ClosureState.InOpening;
		try {
			load();
		}
		catch( IOException e ) {
			throw new FileSystemException("Can not load the entries of file_table and directory_table!", e);
		}
		deduplication_manager.open();
		closure_state = ClosureState.Open;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.close.IClose#close()
	 */
	@Override public void close() {
		if( !isOpen() ) {
			if( StaticMode.TESTING_MODE ) {
				throw new FileSystemException("The FileSystem is not open!");
			}
			return;
		}
		closure_state = ClosureState.InClosure;
		deduplication_manager.close();
		closure_state = ClosureState.Closed;
	}
	
}
