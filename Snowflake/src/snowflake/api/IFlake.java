package snowflake.api;

import java.io.IOException;

import j3l.util.check.IValidate;
import j3l.util.close.IStateClosure;
import snowflake.core.IChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.03_0
 * @author Johannes B. Latzel
 */
public interface IFlake extends IStateClosure, IValidate {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean delete();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getLength();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void setLength(long new_length);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean isWriting();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int getNumberOfChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean isDamaged();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IChunk[] getChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IChunk getChunkAtIndex(int index);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IChunk getChunkAtPositionInFlake(long position_in_flake);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	FlakeInputStream getFlakeInputStream() throws IOException;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	FlakeOutputStream getFlakeOutputStream() throws IOException;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	long getIdentification();
	
	
	/**
	 * <p></p>
	 */
	boolean isDeleted();
	
}
