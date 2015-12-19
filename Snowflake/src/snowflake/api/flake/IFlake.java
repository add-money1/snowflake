package snowflake.api.flake;

import java.io.IOException;

import j3l.util.check.IValidate;
import j3l.util.close.IStateClosure;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.stream.FlakeInputStream;
import snowflake.api.stream.FlakeOutputStream;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.18_0
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
	default boolean isStreamable() {
		return isValid() && !isChunkMerging() && !isLocked();
	}
	
	
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
	boolean isChunkMerging();
	
	
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
	void mergeChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	boolean needsChunkMerging();
	
	
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
	IChunkInformation[] getChunks();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IChunkInformation getChunkAtIndex(int index);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	IChunkInformation getChunkAtPositionInFlake(long position_in_flake);
	
	
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
	boolean isLocked();
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Lock lock(Object owner);
	
	
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
