package snowflake.core.storage;

import j3l.util.check.ArgumentChecker;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.02.22_0
 * @author Johannes B. Latzel
 */
public enum StorageConfigurationElement {
	
	
	PreferredAvailableStorageSize("preferred_available_storage_size"),
	DataFileIncreaseRate("data_file_increase_rate"),
	ChunkTableFilePath("chunk_table_file_path"),
	ChunkManagerIndexConfigurationFilePath("chunk_manager_index_configuration_file_path"),
	DefragmentationTransferBufferSize("defragmentation_transfer_buffer_size"),
	DefragmentationChunkSizeTreshhold("defragmentation_chunk_size_treshhold"),
	InitializationFilePath("initialization_file_path"),
	DataFilePath("data_file_path"),
	ConfigurationFilePath("configuration_file_path"),
	ClearArraySize("clear_array_size"),
	MaximumAvailableChunks("maximum_available_chunks"),
	ChunkRecyclingTreshhold("chunk_recycling_treshhold");
	
	
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
		this.name = ArgumentChecker.checkForEmptyString(name, "name");
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
