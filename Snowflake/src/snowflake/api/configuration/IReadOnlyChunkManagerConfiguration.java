package snowflake.api.configuration;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.14_0
 * @author Johannes B. Latzel
 */
public interface IReadOnlyChunkManagerConfiguration {
	
	long getPreferredAvailableStorageSize();
	double getDataFileIncreaseRate();
	int getMaximumChunkDataTableSize();
	int getMaximumAvailableChunks();
	String getChunkTableFilePath();
	String getChunkManagerIndexConfigurationFilePath();

}
