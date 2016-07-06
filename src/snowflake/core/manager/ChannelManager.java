package snowflake.core.manager;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.api.StorageException;
import snowflake.core.Channel;
import snowflake.core.Returnable;
import snowflake.core.storage.IChannelManagerConfiguration;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.07_0
 * @author Johannes B. Latzel
 */
public final class ChannelManager implements Closeable, IChannelManager {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Channel> available_channel_list;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Channel> unavailable_channel_list;
	
	
	/**
	 * <p></p>
	 */
	private final IChannelManagerConfiguration channel_manager_configuration;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_closed;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChannelManager(IChannelManagerConfiguration channel_manager_configuration) {
		this.channel_manager_configuration = ArgumentChecker.checkForNull(
				channel_manager_configuration, GlobalString.ChannelManagerConfiguration.toString()
		);
		available_channel_list = new ArrayList<>();
		unavailable_channel_list = new ArrayList<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws FileNotFoundException 
	 */
	@SuppressWarnings("resource")
	private void createChannel() {
		RandomAccessFile random_access_file;
		try {
			random_access_file = new RandomAccessFile(channel_manager_configuration.getDataFilePath(), "rw");
		}
		catch( FileNotFoundException e ) {
			throw new StorageException("Can not create a new " + GlobalString.Channel.toString()
					+ ", because the " + GlobalString.DataFile.toString() + " does not exist!", e);
		}
		synchronized (available_channel_list) {
			available_channel_list.add(new Channel(random_access_file));
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.core.manager.IGetChannel#getChannel()
	 */
	@SuppressWarnings("resource")
	@Override public Channel getChannel() {
		Channel channel = null;
		while( !is_closed ) {
			synchronized( available_channel_list ) {
				if( !available_channel_list.isEmpty() ) {
					channel = available_channel_list.remove(0);
					break;
				}
			}
			createChannel();
		}
		synchronized( unavailable_channel_list ) {
			if( unavailable_channel_list.contains(channel) ) {
				throw new StorageException("A " + GlobalString.Channel.toString() 
					+ " must never be used by multiple instances simultaniously!");
			}
			if( !unavailable_channel_list.add(channel) ) {
				throw new StorageException("A " + GlobalString.Channel.toString() + " got lost on its way!");
			}
		}
		return channel;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.core.manager.IReturnChannel#returnChannel(snowflake.core.Returnable)
	 */
	@Override public void returnChannel(Returnable channel) throws IOException {
		ArgumentChecker.checkForNull(channel, GlobalString.Channel.toString());
		if( !(channel instanceof Channel) ) {
			throw new StorageException("The returnable is no instance of Channel!");
		}
		Channel actual_channel = (Channel)channel;
		synchronized( unavailable_channel_list ) {
			if( !unavailable_channel_list.contains(actual_channel) ) {
				throw new StorageException("The " + GlobalString.Channel.toString() + "does not belong in here!");
			}
			if( !unavailable_channel_list.remove(actual_channel) ) {
				throw new StorageException("The " + GlobalString.Channel.toString() + "could not be returned.");
			}
		}
		synchronized( available_channel_list ) {
			if( available_channel_list.size() < channel_manager_configuration.getMaximumNumberOfAvailableChannel() ) {
				if( !available_channel_list.add(actual_channel) ) {
					throw new StorageException("The " + GlobalString.Channel.toString() + "could not be returned.");
				}
				return;
			}
		}
		try {
			actual_channel.close();
		}
		catch( IOException e ) {
			throw new IOException("Could not return the Channel!", e);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@SuppressWarnings("resource")
	@Override public void close() throws IOException {
		if( is_closed ) {
			return;
		}
		is_closed = true;
		synchronized( available_channel_list ) {
			for( Channel channel : available_channel_list ) {
				try {
					channel.close();
				}
				catch( IOException e ) {
					throw new IOException("Could not close the channel " + channel.toString() + "!", e);
				}
			}
			available_channel_list.clear();
		}
		synchronized( unavailable_channel_list ) {
			for( Channel channel : unavailable_channel_list ) {
				try {
					channel.close();
				}
				catch( IOException e ) {
					throw new IOException("Could not close the channel " + channel.toString() + "!", e);
				}
			}
			unavailable_channel_list.clear();
		}
	}
	
}
