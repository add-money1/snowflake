package snowflake.core.flake;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.stream.Stream;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.IStateClosure;
import snowflake.api.GlobalString;
import snowflake.api.stream.FlakeInputStream;
import snowflake.api.stream.FlakeOutputStream;
import snowflake.core.storage.IRead;
import snowflake.core.storage.IWrite;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class FlakeStreamManager implements IFlakeStreamManager {
	
	
	/**
	 * <p></p>
	 */
	private final Object stream_creation_lock;
	
	
	/**
	 * <p></p>
	 */
	private final IWrite write;
	
	
	/**
	 * <p></p>
	 */
	private final IRead read;
	
	
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
	public FlakeStreamManager(IRead read, IWrite write) {
		this.read = ArgumentChecker.checkForNull(read, GlobalString.Read.toString());
		this.write = ArgumentChecker.checkForNull(write, GlobalString.Write.toString());
		stream_creation_lock = new Object();
		stream_list = null;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.core.flake.IFlakeStreamManager#removeStream(j3l.util.interfaces.IStateClosure)
	 */
	@Override public void removeStream(IStateClosure closeable_stream) {
		if( (closeable_stream instanceof FlakeOutputStream || closeable_stream instanceof FlakeInputStream)
				&& closeable_stream.isClosed() ) {
			synchronized( stream_creation_lock ) {
				stream_list.remove(closeable_stream);
			}
		}
		
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
			FlakeOutputStream stream = new FlakeOutputStream(flake, write, this);
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
			FlakeInputStream stream = new FlakeInputStream(flake, read, this);
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
	
}
