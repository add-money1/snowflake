package snowflake.api;

import java.io.IOException;

import j3l.util.IValidate;
import j3l.util.IStateClosure;
import snowflake.core.IChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void cutFromStart(long number_of_bytes);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void cutFromEnd(long number_of_bytes);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void cutAt(long position_in_flake, long number_of_bytes);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void expandAtStart(long number_of_bytes);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void expandAtEnd(long number_of_bytes);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void expandAt(long position_in_flake, long number_of_bytes);
	
}
