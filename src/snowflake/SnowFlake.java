package snowflake;

import java.io.IOException;
import java.nio.file.Path;

import snowflake.api.IStorageInformation;
import snowflake.core.storage.Storage;
import snowflake.core.storage.StorageConfiguration;
import snowflake.filesystem.FileSystem;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.29_0
 * @author Johannes B. Latzel
 */
public final class SnowFlake implements AutoCloseable {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static Storage format(Path path) {
		// erstelle Storage in Datei oder Datenträger path
		
		// tabelle in blockform über speicher verteilt, sodass storage in einer datei, bzw
		// auf einem Datenträger abgelegt werden kann (mergen von storage.data und chunk.table).6.
		// erste n * 33 + 8 Bytes für n chunks
		// letzter eintrag ist pointer für nächste n chunks
		System.out.println("This method has not been implemented!");
		System.out.println(path);
		return null;
	}

	
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
		file_system.open();
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
		file_system.close();
		storage.close();
	}

}
