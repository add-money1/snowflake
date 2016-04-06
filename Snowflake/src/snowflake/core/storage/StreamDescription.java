package snowflake.core.storage;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.time.Instant;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.configuration.IReadonlyStorageConfiguration;
import snowflake.api.storage.StorageException;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.01_0
 * @author Johannes B. Latzel
 */
public final class StreamDescription {
	
	
	/**
	 * <p></p>
	 */
	private long transferred_bytes;
	
	
	/**
	 * <p></p>
	 */
	private final long start_instant;
	
	
	/**
	 * <p></p>
	 */
	private RandomAccessFile data_file;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public StreamDescription() {
		data_file = null;
		transferred_bytes = 0;
		start_instant = Instant.now().toEpochMilli();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean hasDataFile() {
		return data_file != null;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void createDataFile(IReadonlyStorageConfiguration storage_configuration) {
		if( data_file != null ) {
			throw new SecurityException("The " + GlobalString.DataFile.toString() + " already exists!");
		}
		try {
			data_file = new RandomAccessFile(storage_configuration.getDataFilePath(), "rw");
		}
		catch( FileNotFoundException e ) {
			throw new StorageException("Could not create a new " + GlobalString.StreamDescription.toString() + "!", e);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public RandomAccessFile getDataFile() {
		return data_file;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getTimeDelta() {
		return Instant.now().toEpochMilli() - start_instant;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getTransferredBytes() {
		return transferred_bytes;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void increaseTransferredBytes(long number_of_bytes) {
		transferred_bytes += ArgumentChecker.checkForBoundaries(
			number_of_bytes, 1, Long.MAX_VALUE, GlobalString.NumberOfBytes.toString()
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public double getTransferationRate() {
		return (double)transferred_bytes / getTimeDelta();
	}
	
}
