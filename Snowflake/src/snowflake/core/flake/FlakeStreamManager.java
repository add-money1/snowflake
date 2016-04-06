package snowflake.core.flake;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.stream.Stream;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.stream.FlakeInputStream;
import snowflake.api.stream.FlakeOutputStream;
import snowflake.core.storage.IGetIOAccess;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.06_0
 * @author Johannes B. Latzel
 */
public final class FlakeStreamManager implements IRemoveStream {
	
	
	/**
	 * <p></p>
	 */
	private final Object stream_creation_lock;
	
	
	/**
	 * <p></p>
	 */
	private final IGetIOAccess io_access_getter;
	
	
	/**
	 * <p></p>
	 */
	private LinkedList<Closeable> stream_list;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeStreamManager(IGetIOAccess io_access_getter) {
		this.io_access_getter = ArgumentChecker.checkForNull(io_access_getter, GlobalString.IOAccessGetter.toString());
		stream_creation_lock = new Object();
		stream_list = null;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isWriting() {
		synchronized( stream_creation_lock ) {
			if( stream_list != null && stream_list.size() > 0 ) {
				for( Closeable stream : stream_list ) {
					if( stream instanceof FlakeOutputStream ) {
						return true;
					}
				}
			}
			return false;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeOutputStream getFlakeOutputStream(Flake flake) {
		synchronized( stream_creation_lock ) {	
			if( stream_list == null ) {
				stream_list = new LinkedList<>();
			}
			FlakeOutputStream stream = new FlakeOutputStream(flake, io_access_getter.getIOAccess(), this);
			stream_list.add(stream);
			return stream;	
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeInputStream getFlakeInputStream(Flake flake) {		
		synchronized( stream_creation_lock ) {	
			if( stream_list == null ) {
				stream_list = new LinkedList<>();
			}
			FlakeInputStream stream = new FlakeInputStream(flake, io_access_getter.getIOAccess(), this);
			stream_list.add(stream);
			return stream;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public LinkedList<IOException> closeAllStreams() {
		
		LinkedList<IOException> exception_list = new LinkedList<>();
		Stream<Closeable> stream;
		int stream_list_size;
		
		synchronized( stream_creation_lock ) {
			
			stream_list_size = stream_list.size();
			
			if( stream_list_size == 0 ) {
				return exception_list;
			}
			else if( stream_list_size < 3 ) {
				stream = stream_list.stream();
			}
			else {
				stream = stream_list.parallelStream();
			}
			
			stream.forEach(closeable -> {
				try {
					closeable.close();
				} catch (IOException e) {
					exception_list.add(new IOException("Failed to close the stream \""
							+ stream.toString() + "\"!", e));
				}
			});
			
		}
		
		return exception_list;
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.core.flake.IRemoveStream#removeStream(java.io.Closeable)
	 */
	@Override public void removeStream(Closeable closeable_stream) {
		if( (closeable_stream instanceof FlakeOutputStream || closeable_stream instanceof FlakeInputStream) ) {
			synchronized( stream_creation_lock ) {
				stream_list.remove(closeable_stream);
			}
		}
		
	}
	
}
