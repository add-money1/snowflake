package snowflake.api.configuration;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.02.22_0
 * @author Johannes B. Latzel
 */
public interface IReadOnlyChunkManagerConfiguration {
	
	long getPreferredAvailableStorageSize();
	double getDataFileIncreaseRate();
	int getMaximumAvailableChunks();
	String getChunkTableFilePath();
	long getChunkRecyclingTreshhold();

}
