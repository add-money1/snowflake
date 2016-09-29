package snowflake.filesystem;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.Util;
import snowflake.api.DataPointer;
import snowflake.api.IAttributeValue;
import snowflake.api.IFlake;
import snowflake.api.StorageException;
import snowflake.core.FlakeInputStream;
import snowflake.core.FlakeOutputStream;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.29_0
 * @author Johannes B. Latzel
 */

public final class AttributeUtility {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private static Attribute createAttribute(String name, String type_name, ByteBuffer value_buffer) {
		// get class
		Class<?> attribute_value_class;
		try {
			attribute_value_class = ClassLoader.getSystemClassLoader().loadClass(type_name);
		}
		catch( ClassNotFoundException e ) {
			throw new StorageException("Could not load the class \"" + type_name + "\"!", e);
		}
		// check if the class is an instance of "snowflake.core.IBinaryData"
		Class<?>[] interfaces = attribute_value_class.getInterfaces();
		if( interfaces == null || interfaces.length == 0 ) {
			throw new StorageException("The class \"" + type_name + "\" has not implemented the interface "
					+ " \"snowflake.api.IAttributeValue\"!");
		}
		boolean found_interface = false;
		for( Class<?> i : interfaces ) {
			if( i.equals(IAttributeValue.class) ) {
				found_interface = true;
				break;
			}
		}
		if( !found_interface ) {
			throw new StorageException("The class \"" + type_name + "\" has not implemented the interface "
					+ " \"snowflake.api.IAttributeValue\"!");
		}
		// get the constructor
		Constructor<?> constructor;
		try {
			constructor = attribute_value_class.getConstructor(byte[].class);
		}
		catch( NoSuchMethodException e ) {
			throw new StorageException("The class \"" + type_name + "\" has no Constructor(byte[])!", e);
		}
		catch (SecurityException e) {
			throw new StorageException(
				"The class \"" + type_name + "\" has no accesseable Constructor(byte[])!", e
			);
		}
		// construct attribute
		Attribute attribute; 
		try {
			attribute = new Attribute(
				name, (IAttributeValue<?>)constructor.newInstance(value_buffer.array())
			);
		}
		catch( InstantiationException e ) {
			throw new StorageException("The class \"" + type_name + "\" must not be abstract!", e);
		}
		catch( IllegalAccessException e ) {
			throw new StorageException(
				"The constructor \"" + constructor.toString() + "\" is not accessable!",  e
			);
		}
		catch( IllegalArgumentException e ) {
			throw new StorageException(
				"The constructor \"" + constructor.toString() + "\" seems to not take an byte[] as an "
				+ "argument - this has been checked and should therefore not happen.", e
			);
		}
		catch( InvocationTargetException e ) {
			throw new StorageException(
				"The constructor \"" + constructor.toString() + "\" threw an exception!",  e
			);
		}
		return attribute;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private static boolean skipIfNotEqual(String name, String read_in_name,
			AttributeHeader current_header, DataPointer pointer) {
		// is it the searched for attribute?
		if( !name.equals(read_in_name) ) {
			// no: skip bytes and continue searching
			int difference = current_header.getTypeNameLength() + current_header.getValueLength();
			if( pointer.getRemainingBytes() < difference ) {
				pointer.seekEOF();
			}
			else {
				pointer.changePosition(difference);
			}
			return true;
		}
		return false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static Attribute loadAttribute(String name, IFlake attribute_flake) {
		Checker.checkForEmptyString(name, GlobalString.Name.toString());
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		DataPointer pointer;
		ByteBuffer header_buffer = AttributeHeader.createBuffer();
		AttributeHeader current_header;
		String read_in_name;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				header_buffer.rewind();
				Util.readComplete(input, header_buffer);
				header_buffer.rewind();
				current_header = AttributeHeader.create(header_buffer);
				ByteBuffer name_buffer = ByteBuffer.allocate(current_header.getNameLength());
				Util.readComplete(input, name_buffer);
				read_in_name = new String(name_buffer.array());
				if( skipIfNotEqual(name, read_in_name, current_header, pointer) ) {
					continue;
				}
				name_buffer = ByteBuffer.allocate(current_header.getTypeNameLength());
				Util.readComplete(input, name_buffer);
				ByteBuffer value_buffer = Util.readComplete(
					input,
					ByteBuffer.allocate(current_header.getValueLength())
				);
				return AttributeUtility.createAttribute(
					read_in_name,
					new String(name_buffer.array()),
					value_buffer
				);
			}
			
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attribute \"" + name + "\"!", e);
		}
		return null;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static List<Attribute> loadAllAttributes(IFlake attribute_flake) {
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		DataPointer pointer;
		ArrayList<Attribute> list = new ArrayList<>(0);
		ByteBuffer header_buffer = AttributeHeader.createBuffer();
		AttributeHeader current_header;
		String read_in_name;
		ByteBuffer name_buffer;
		ByteBuffer value_buffer;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				header_buffer.rewind();
				Util.readComplete(input, header_buffer);
				header_buffer.rewind();
				current_header = AttributeHeader.create(header_buffer);
				name_buffer = ByteBuffer.allocate(current_header.getNameLength());
				Util.readComplete(input, name_buffer);
				read_in_name = new String(name_buffer.array());
				name_buffer = ByteBuffer.allocate(current_header.getTypeNameLength());
				Util.readComplete(input, name_buffer);
				value_buffer = Util.readComplete(
					input,
					ByteBuffer.allocate(current_header.getValueLength())
				);
				list.add(AttributeUtility.createAttribute(
					read_in_name,
					new String(name_buffer.array()),
					value_buffer
				));
			}
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attributes!", e);
		}
		list.trimToSize();
		return list;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static List<Attribute> loadNecessaryAttributes(IFlake attribute_flake, List<Attribute> attribute_list) {
		if( attribute_list == null || attribute_list.isEmpty() ) {
			return AttributeUtility.loadAllAttributes(attribute_flake);
		}
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		DataPointer pointer;
		ArrayList<Attribute> list = new ArrayList<>(0);
		ArrayList<Attribute> known_attribute_list = new ArrayList<>(attribute_list);
		ByteBuffer header_buffer = AttributeHeader.createBuffer();
		AttributeHeader current_header;
		String read_in_name;
		ByteBuffer name_buffer;
		ByteBuffer value_buffer;
		boolean attribute_is_cached;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				header_buffer.rewind();
				Util.readComplete(input, header_buffer);
				header_buffer.rewind();
				current_header = AttributeHeader.create(header_buffer);
				name_buffer = ByteBuffer.allocate(current_header.getNameLength());
				Util.readComplete(input, name_buffer);
				read_in_name = new String(name_buffer.array());
				attribute_is_cached = false;
				for( Attribute a : known_attribute_list ) {
					if( a.getName().equals(read_in_name) ) {
						// yes: skip bytes and continue searching
						pointer.changePosition(current_header.getTypeNameLength() + current_header.getValueLength());
						attribute_is_cached = true;
						known_attribute_list.remove(a);
						break;
					}
				}
				if( attribute_is_cached ) {
					continue;
				}
				name_buffer = ByteBuffer.allocate(current_header.getTypeNameLength());
				Util.readComplete(input, name_buffer);
				value_buffer = Util.readComplete(
					input,
					ByteBuffer.allocate(current_header.getValueLength())
				);
				list.add(AttributeUtility.createAttribute(
					read_in_name,
					new String(name_buffer.array()),
					value_buffer
				));
			}
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attributes!", e);
		}
		list.trimToSize();
		return list;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static void setAttribute(Attribute attribute, IFlake attribute_flake) {
		Checker.checkForNull(attribute, GlobalString.Attribute.toString());
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		DataPointer pointer;
		ByteBuffer header_buffer = AttributeHeader.createBuffer();
		ByteBuffer name_buffer;
		String read_in_name;
		String name = attribute.getName();
		boolean override = false;
		AttributeHeader current_header = null;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				header_buffer.rewind();
				Util.readComplete(input, header_buffer);
				header_buffer.rewind();
				current_header = AttributeHeader.create(header_buffer);
				name_buffer = ByteBuffer.allocate(current_header.getNameLength());
				Util.readComplete(input, name_buffer);
				read_in_name = new String(name_buffer.array());
				if( skipIfNotEqual(name, read_in_name, current_header, pointer) ) {
					continue;
				}
				override = true;
				break;
			}
			IAttributeValue<?> attribute_value = attribute.getAttributeValue();
			String type_name = attribute_value.getClass().getName();
			// the data and header capacity in bytes needed to save this attribute
			name_buffer = ByteBuffer.wrap(name.getBytes());
			ByteBuffer type_name_buffer = ByteBuffer.wrap(type_name.getBytes());
			int new_value_length = attribute_value.getDataLength();
			if( override ) {
				pointer.changePosition( -name_buffer.capacity() - AttributeHeader.SIZE );
				// difference = current_capacity - needed_capacity
				int difference = -attribute.getBinarySize();
				if( current_header != null ) {
					difference += current_header.getAttributeLength();
				}
				long current_position = pointer.getPositionInFlake();
				if( difference > 0 ) {
					attribute_flake.cutAt(current_position, difference);
				}
				else if( difference < 0 ) {
					// number_of_bytes must be positive
					attribute_flake.expandAt(current_position, -difference);
				}
			}
			else {
				if( !pointer.isEOF() ) {
					throw new StorageException("The attribute is neither an override nor is pointer at "
						+ "the end of the flake."
					);
				}
				attribute_flake.expandAtEnd(new_value_length);
			}
			try( FlakeOutputStream output = attribute_flake.getFlakeOutputStream() ) {
				output.getDataPointer().setPosition(pointer.getPositionInFlake());
				header_buffer.rewind();
				header_buffer.putShort((short)name_buffer.capacity());
				header_buffer.putShort((short)type_name_buffer.capacity());
				header_buffer.putInt(new_value_length);
				header_buffer.flip();
				output.write(header_buffer);
				name_buffer.rewind();
				output.write(name_buffer);
				type_name_buffer.rewind();
				output.write(type_name_buffer);
				output.write(ByteBuffer.wrap(attribute_value.getBinaryData()));
			}
			catch( Exception e ) {
				throw e;
			}
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attribute \"" + name + "\"!", e);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static void removeAttribute(String attribute_name, IFlake attribute_flake) {
		Checker.checkForEmptyString(attribute_name, GlobalString.AttributeName.toString());
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		DataPointer pointer;
		ByteBuffer header_buffer = AttributeHeader.createBuffer();
		ByteBuffer name_buffer;
		String read_in_name;
		long position_of_attribute = -1;
		long length_of_attribute = -1;
		AttributeHeader current_header;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				header_buffer.rewind();
				Util.readComplete(input, header_buffer);
				header_buffer.rewind();
				current_header = AttributeHeader.create(header_buffer);
				name_buffer = ByteBuffer.allocate(current_header.getNameLength());
				read_in_name = new String(name_buffer.array());
				if( skipIfNotEqual(attribute_name, read_in_name, current_header, pointer) ) {
					continue;
				}
				position_of_attribute = pointer.getPositionInFlake() - current_header.getNameLength()
						- AttributeHeader.SIZE;
				length_of_attribute = AttributeHeader.SIZE + current_header.getNameLength()
						+ current_header.getTypeNameLength() + current_header.getValueLength();
				break;
			}
			if( position_of_attribute < 0 ) {
				throw new IllegalArgumentException("The attribute_flake does not contain the attribute \""
						+ attribute_name + "\"!");
			}
			attribute_flake.cutAt(position_of_attribute, length_of_attribute);
		}
		catch( Exception e ) {
			throw new StorageException("Could remove the attribute \"" + attribute_name + "\"!", e);
		}
	}
	
}
