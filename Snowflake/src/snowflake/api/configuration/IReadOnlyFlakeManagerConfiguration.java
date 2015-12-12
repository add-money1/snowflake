package snowflake.api.configuration;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.23_0
 * @author Johannes B. Latzel
 */
public interface IReadOnlyFlakeManagerConfiguration {
	
	int getDefragmentationTransferBufferSize();
	long getDefragmentationChunkSizeTreshhold();

}
