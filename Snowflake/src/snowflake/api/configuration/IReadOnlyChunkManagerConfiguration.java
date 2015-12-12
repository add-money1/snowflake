package snowflake.api.configuration;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.29_0
 * @author Johannes B. Latzel
 */
public interface IReadOnlyChunkManagerConfiguration {
	
	long getPreferredAvailableStorageSize();
	double getDataFileIncreaseRate();
	int getMaximumChunkDataTableSize();
	String getChunkTableFilePath();
	String getChunkManagerIndexConfigurationFilePath();

}
