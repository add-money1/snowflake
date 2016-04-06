package snowflake.core.flake;

import java.io.Closeable;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.04.06_0
 * @author Johannes B. Latzel
 */
public interface IRemoveStream {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void removeStream(Closeable closeable_stream);
	
}
