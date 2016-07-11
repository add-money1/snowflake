package snowflake.core.storage;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public enum StorageConfigurationElement {
	
	
	PreferredAvailableStorageSize("preferred_available_storage_size"),
	DataFileIncreaseRate("data_file_increase_rate"),
	ChunkTableFilePath("chunk_table_file_path"),
	DefragmentationTransferBufferSize("defragmentation_transfer_buffer_size"),
	DefragmentationChunkSizeTreshhold("defragmentation_chunk_size_treshhold"),
	DataFilePath("data_file_path"),
	ClearArraySize("clear_array_size"),
	MaximumAvailableChunks("maximum_available_chunks"),
	ChunkRecyclingTreshhold("chunk_recycling_treshhold"), 
	MaximumStorageSize("maximum_storage_size"),
	MaximumNumberOfAvailableChannel("maximum_number_of_available_channel");
	
	
	/**
	 * <p></p>
	 */
	private final String name;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private StorageConfigurationElement(String name) {
		if( StaticMode.TESTING_MODE ) {
			this.name = Checker.checkForEmptyString(name, GlobalString.Name.toString());
		}
		else {
			this.name = name;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	String getName() {
		return name;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override public String toString() {
		return getName();
	}	
	
}
