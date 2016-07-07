package snowflake.core.storage;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.12_0
 * @author Johannes B. Latzel
 */
public interface IStorageConfiguration extends IChunkManagerConfiguration, 
												IFlakeManagerConfiguration, IChannelManagerConfiguration {
	
	int getClearArraySize();
	long getMaximumStorageSize();
	
}
