package snowflake.core.storage;


import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import j3l.configuration.ConfigurationManager;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.StaticMode;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.08_0
 * @author Johannes B. Latzel
 */
public final class StorageConfiguration implements IStorageConfiguration {
	
	
	/**
	 * <p></p>
	 */
	private final ConfigurationManager configuration_manager;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public StorageConfiguration(File configuration_file) throws IOException {
		if( StaticMode.TESTING_MODE ) {
			ArgumentChecker.checkForNull(configuration_file, GlobalString.ConfigurationFile.toString());
		}
		if( !configuration_file.exists() ) {
			try {
				configuration_file.createNewFile();
			} catch (IOException e) {
				throw new IOException("Failed to create the configuration_file \""
						+ configuration_file.getAbsolutePath() + "\"!", e);
			}
		}
		configuration_manager = new ConfigurationManager(configuration_file);
		try {
			configuration_manager.loadConfiguration();
		} catch (IOException e) {
			throw new IOException("Failed to load the configuration_manager!", e);
		}
	}	
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private int getValue(String name, int default_value) {
		ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		String value_string = configuration_manager.getValue(name);
		int actual_value = default_value;
		if( !value_string.equals("") ) {
			try {
				actual_value = Integer.parseInt(value_string);
			}
			catch ( NumberFormatException e ) {
				e.printStackTrace();
			}
		}
		else {
			configuration_manager.setElement(name, Integer.toString(actual_value));
		}
		return actual_value;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private long getValue(String name, long default_value) {
		ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		String value_string = configuration_manager.getValue(name);
		long actual_value = default_value;
		if( !value_string.equals("") ) {
			try {
				actual_value = Long.parseLong(value_string);
			}
			catch ( NumberFormatException e ) {
				e.printStackTrace();
			}
		}
		else {
			configuration_manager.setElement(name, Long.toString(actual_value));
		}
		return actual_value;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private double getValue(String name, double default_value) {
		ArgumentChecker.checkForEmptyString(name, GlobalString.Name.toString());
		String value_string = configuration_manager.getValue(name);
		double actual_value = default_value;
		if( !value_string.equals("") ) {
			try {
				actual_value = Double.parseDouble(value_string);
			}
			catch ( NumberFormatException e ) {
				e.printStackTrace();
			}
		}
		else {
			configuration_manager.setElement(name, Double.toString(actual_value));
		}
		return actual_value;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private String getValue(String name, String default_value) {
		String value_string = configuration_manager.getValue(name);
		String actual_value = default_value;
		if( value_string.equals("") ) {
			configuration_manager.setElement(name, default_value);
		}
		else {
			actual_value = value_string;
		}
		return actual_value;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public void saveConfiguration() throws IOException {
		configuration_manager.saveConfiguration();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean createFiles() throws IOException {
		
		boolean success = true;
		
		LinkedList<String> file_path_list = new LinkedList<>();
		file_path_list.add(getChunkTableFilePath());
		file_path_list.add(getDataFilePath());
		
		for( String file_path : file_path_list ) {
			success &= (new File(file_path)).createNewFile();
		}
		
		return success;
		
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setDefragmentationTransferBufferSize(int defragmentation_transfer_buffer_size) {
		configuration_manager.setElement(
			StorageConfigurationElement.DefragmentationTransferBufferSize.getName(), 
			Integer.toString(
				ArgumentChecker.checkForBoundaries(
					defragmentation_transfer_buffer_size, 
					1,
					Integer.MAX_VALUE, 
					StorageConfigurationElement.DefragmentationTransferBufferSize.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setClearArraySize(int clear_array_size) {
		configuration_manager.setElement(
			StorageConfigurationElement.ClearArraySize.getName(), 
			Integer.toString(
				ArgumentChecker.checkForBoundaries(
					clear_array_size, 
					1,
					Integer.MAX_VALUE, 
					StorageConfigurationElement.ClearArraySize.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setMaximumAvailableChunks(int maximum_available_chunks) {
		configuration_manager.setElement(
			StorageConfigurationElement.MaximumAvailableChunks.getName(), 
			Integer.toString(
				ArgumentChecker.checkForBoundaries(
					maximum_available_chunks, 
					1,
					Integer.MAX_VALUE, 
					StorageConfigurationElement.MaximumAvailableChunks.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setMaximumNumberOfAvailableChannel(int maximum_number_of_available_channel) {
		configuration_manager.setElement(
			StorageConfigurationElement.MaximumNumberOfAvailableChannel.getName(), 
			Integer.toString(
				ArgumentChecker.checkForBoundaries(
					maximum_number_of_available_channel, 
					1,
					Integer.MAX_VALUE, 
					StorageConfigurationElement.MaximumNumberOfAvailableChannel.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setPreferredAvailableStorageSize(long preferred_available_storage_size) {
		configuration_manager.setElement(
			StorageConfigurationElement.PreferredAvailableStorageSize.getName(), 
			Long.toString(
				ArgumentChecker.checkForBoundaries(
					preferred_available_storage_size, 
					0, 
					Long.MAX_VALUE, 
					StorageConfigurationElement.PreferredAvailableStorageSize.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setDefragmentationChunkSizeTreshhold(long defragmentation_chunk_size_treshhold) {
		configuration_manager.setElement(
			StorageConfigurationElement.DefragmentationChunkSizeTreshhold.getName(), 
			Long.toString(
				ArgumentChecker.checkForBoundaries(
					defragmentation_chunk_size_treshhold, 
					1, 
					Long.MAX_VALUE, 
					StorageConfigurationElement.DefragmentationChunkSizeTreshhold.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setChunkRecyclingTreshhold(long chunk_recycling_treshhold) {
		configuration_manager.setElement(
			StorageConfigurationElement.ChunkRecyclingTreshhold.getName(), 
			Long.toString(
				ArgumentChecker.checkForBoundaries(
					chunk_recycling_treshhold, 
					1, 
					Long.MAX_VALUE, 
					StorageConfigurationElement.ChunkRecyclingTreshhold.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setMaximumStorageSize(long maximum_storage_size) {
		configuration_manager.setElement(
			StorageConfigurationElement.MaximumStorageSize.getName(), 
			Long.toString(
				ArgumentChecker.checkForBoundaries(
					maximum_storage_size, 
					1, 
					Long.MAX_VALUE, 
					StorageConfigurationElement.MaximumStorageSize.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setDataFileIncreaseRate(double data_file_increase_rate) {
		configuration_manager.setElement(
			StorageConfigurationElement.DataFileIncreaseRate.getName(), 
			Double.toString(
				ArgumentChecker.checkForBoundaries(
					data_file_increase_rate, 
					0, 
					Double.MAX_VALUE, 
					StorageConfigurationElement.DataFileIncreaseRate.getName()
				)
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setChunkTableFilePath(String chunk_table_file_path) {
		configuration_manager.setElement(
			StorageConfigurationElement.ChunkTableFilePath.getName(), 
			ArgumentChecker.checkForEmptyString(
				chunk_table_file_path, 
				StorageConfigurationElement.ChunkTableFilePath.getName()
			)
		);
	}
	
	
	/**
	 * <p></p>
	 *s
	 * @param
	 * @return
	 */
	public void setDataFilePath(String data_file_path) {
		configuration_manager.setElement(
			StorageConfigurationElement.DataFilePath.getName(), 
			ArgumentChecker.checkForEmptyString(
				data_file_path, 
				StorageConfigurationElement.DataFilePath.getName()
			)
		);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChunkManagerConfiguration#getPreferredAvailableStorageSize()
	 */
	@Override public long getPreferredAvailableStorageSize() {
		return getValue(StorageConfigurationElement.PreferredAvailableStorageSize.toString(), 0L);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChunkManagerConfiguration#getDataFileIncreaseRate()
	 */
	@Override public double getDataFileIncreaseRate() {
		return getValue(StorageConfigurationElement.DataFileIncreaseRate.toString(), 0.1d);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChunkManagerConfiguration#getChunkTableFilePath()
	 */
	@Override public String getChunkTableFilePath() {
		return getValue(StorageConfigurationElement.ChunkTableFilePath.toString(), "");
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyFlakeManagerConfiguration#getDefragmentationTransferBufferSize()
	 */
	@Override public int getDefragmentationTransferBufferSize() {
		return getValue(StorageConfigurationElement.DefragmentationTransferBufferSize.toString(), 8192);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyFlakeManagerConfiguration#getDefragmentationChunkSizeTreshhold()
	 */
	@Override public long getDefragmentationChunkSizeTreshhold() {
		return getValue(StorageConfigurationElement.DefragmentationChunkSizeTreshhold.toString(), 200_000L);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadonlyStorageConfiguration#getDataFilePath()
	 */
	@Override public String getDataFilePath() {
		return getValue(StorageConfigurationElement.DataFilePath.toString(), "");
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadonlyStorageConfiguration#getClearArraySize()
	 */
	@Override public int getClearArraySize() {
		return getValue(StorageConfigurationElement.ClearArraySize.toString(), 8192);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChunkManagerConfiguration#getTresholdForChunkMerging()
	 */
	@Override public int getMaximumAvailableChunks() {
		return getValue(StorageConfigurationElement.MaximumAvailableChunks.toString(), 1000);
	}


	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChannelManagerConfiguration#getMaximumNumberOfAvailableChannel()
	 */
	@Override public int getMaximumNumberOfAvailableChannel() {
		return getValue(StorageConfigurationElement.MaximumNumberOfAvailableChannel.toString(), 10);
	}


	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadOnlyChunkManagerConfiguration#getChunkRecyclingTreshhold()
	 */
	@Override public long getChunkRecyclingTreshhold() {
		return getValue(StorageConfigurationElement.ChunkRecyclingTreshhold.toString(), 1_000_000);
	}


	/* (non-Javadoc)
	 * @see snowflake.api.configuration.IReadonlyStorageConfiguration#getMaximumStorageSize()
	 */
	@Override public long getMaximumStorageSize() {
		return getValue(StorageConfigurationElement.MaximumStorageSize.toString(), Long.MAX_VALUE);
	}
	
}
