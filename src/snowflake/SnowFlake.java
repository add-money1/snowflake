package snowflake;

import java.io.IOException;

import snowflake.api.IStorageInformation;
import snowflake.core.storage.Storage;
import snowflake.core.storage.StorageConfiguration;
import snowflake.filesystem.FileSystem;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.11_0
 * @author Johannes B. Latzel
 */
public final class SnowFlake implements AutoCloseable {

	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private final Storage storage;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private final FileSystem file_system;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 * @throws IOException
	 */
	public SnowFlake(StorageConfiguration storage_configuration) throws IOException {
		storage = new Storage(storage_configuration);
		storage.open();
		file_system = new FileSystem(storage);
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IStorageInformation getStorageInformation() {
		return storage;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.AutoCloseable#close()
	 */
	@Override public void close() throws IOException {
		storage.close();
	}

}
