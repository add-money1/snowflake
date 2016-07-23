package snowflake.filesystem;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import j3l.util.InputUtility;
import j3l.util.TransformValue2;
import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.DataPointer;
import snowflake.api.FlakeInputStream;
import snowflake.api.FlakeOutputStream;
import snowflake.api.IAttributeValue;
import snowflake.api.IFlake;
import snowflake.api.StorageException;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.23_0
 * @author Johannes B. Latzel
 */

public final class AttributeUtility {
	
	
	/**
	 * <p></p>
	 */
	private final static int ATTRIBUTE_HEADER_LENGTH = 8;	
	
	
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
		int name_length;
		int type_name_length;
		int value_length;
		byte[] short_buffer = new byte[2];
		byte[] int_buffer = new byte[4];
		String read_in_name;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				name_length = TransformValue2.toShort(InputUtility.readComplete(input, short_buffer));
				type_name_length = TransformValue2.toShort(InputUtility.readComplete(input, short_buffer));
				value_length = TransformValue2.toInteger(InputUtility.readComplete(input, int_buffer));
				read_in_name = new String(InputUtility.readComplete(input, new byte[name_length]));
				// is it the searched for attribute?
				if( !name.equals(read_in_name) ) {
					// no: skip bytes and continue searching
					pointer.changePosition(type_name_length + value_length);
					continue;
				}
				String type_name = new String(InputUtility.readComplete(input, new byte[type_name_length]));
				byte[] value_buffer = InputUtility.readComplete(input, new byte[value_length]);
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
						name, (IAttributeValue<?>)constructor.newInstance(value_buffer)
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
		ArrayList<Attribute> list = new ArrayList<>(10);
		DataPointer pointer;
		int name_length;
		int type_name_length;
		int value_length;
		byte[] short_buffer = new byte[2];
		byte[] int_buffer = new byte[4];
		byte[] value_buffer;
		String name = null;
		String type_name;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				InputUtility.readComplete(input, short_buffer);
				name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, short_buffer);
				type_name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, int_buffer);
				value_length = TransformValue2.toInteger(int_buffer);
				name = new String(InputUtility.readComplete(input, new byte[name_length]));
				type_name = new String(InputUtility.readComplete(input, new byte[type_name_length]));
				value_buffer = InputUtility.readComplete(input, new byte[value_length]);
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
						name, (IAttributeValue<?>)constructor.newInstance(value_buffer)
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
				list.add(attribute);
			}
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attribute \"" + name + "\"!", e);
		}
		return list;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static List<Attribute> loadNecessaryAttributes(IFlake attribute_flake, List<Attribute> attribute_list) {
		Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		ArrayList<Attribute> list = new ArrayList<>(10);
		DataPointer pointer;
		int name_length;
		int type_name_length;
		int value_length;
		byte[] short_buffer = new byte[2];
		byte[] int_buffer = new byte[4];
		String name = null;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				InputUtility.readComplete(input, short_buffer);
				name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, short_buffer);
				type_name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, int_buffer);
				value_length = TransformValue2.toInteger(int_buffer);
				name = new String(InputUtility.readComplete(input, new byte[name_length]));
				// check if the attribute is already there
				boolean attribute_is_cached = false;
				for( Attribute a : attribute_list ) {
					if( a.getName().equals(name) ) {
						// yes: skip bytes and continue searching
						pointer.changePosition(type_name_length + value_length);
						attribute_is_cached = true;
						break;
					}
				}
				if( attribute_is_cached ) {
					continue;
				}
				String type_name = new String(InputUtility.readComplete(input, new byte[type_name_length]));
				byte[] value_buffer = InputUtility.readComplete(input, new byte[value_length]);
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
						name, (IAttributeValue<?>)constructor.newInstance(value_buffer)
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
				list.add(attribute);
			}
		}
		catch( Exception e ) {
			throw new StorageException("Could not load the attribute \"" + name + "\"!", e);
		}
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
		int name_length = 0;
		int type_name_length = 0;
		int value_length = 0;
		byte[] short_buffer = new byte[2];
		byte[] int_buffer = new byte[4];
		String name = null;
		boolean override = false;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				InputUtility.readComplete(input, short_buffer);
				name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, short_buffer);
				type_name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, int_buffer);
				value_length = TransformValue2.toInteger(int_buffer);
				name = new String(InputUtility.readComplete(input, new byte[name_length]));
				// is it the searched for attribute?
				if( !attribute.getName().equals(name) ) {
					// no: skip bytes and continue searching
					pointer.changePosition(type_name_length + value_length);
					continue;
				}
				override = true;
				break;
			}
			IAttributeValue<?> attribute_value = attribute.getAttributeValue();
			String type_name = attribute_value.getClass().getName();
			name = attribute.getName();
			// the data and header capacity in bytes needed to save this attribute
			byte[] new_name_buffer = name.getBytes();
			byte[] new_type_name_buffer = type_name.getBytes();
			int new_value_length = attribute_value.getDataLength();
			int needed_capacity = new_name_buffer.length + new_type_name_buffer.length + new_value_length
					+ AttributeUtility.ATTRIBUTE_HEADER_LENGTH;
			if( override ) {
				pointer.changePosition( -name_length - AttributeUtility.ATTRIBUTE_HEADER_LENGTH );
				// difference = current_capacity - needed_capacity
				int difference = AttributeUtility.ATTRIBUTE_HEADER_LENGTH + name_length + type_name_length
						+ value_length - needed_capacity;
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
				attribute_flake.expandAtEnd(needed_capacity);
			}
			try( FlakeOutputStream output = attribute_flake.getFlakeOutputStream() ) {
				output.getDataPointer().setPosition(pointer.getPositionInFlake());
				output.write(TransformValue2.toByteArray((short)new_name_buffer.length, short_buffer));
				output.write(TransformValue2.toByteArray((short)new_type_name_buffer.length, short_buffer));
				output.write(TransformValue2.toByteArray(new_value_length, int_buffer));
				output.write(new_name_buffer);
				output.write(new_type_name_buffer);
				output.write(attribute_value.getBinaryData());
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
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForEmptyString(attribute_name, GlobalString.AttributeName.toString());
			Checker.checkForNull(attribute_flake, GlobalString.AttributeFlake.toString());
		}
		DataPointer pointer;
		int name_length = 0;
		int type_name_length = 0;
		int value_length = 0;
		byte[] short_buffer = new byte[2];
		byte[] int_buffer = new byte[4];
		String name = null;
		long position_of_attribute = -1;
		long length_of_attribute = -1;
		try( FlakeInputStream input = attribute_flake.getFlakeInputStream() ) {
			pointer = input.getDataPointer();
			pointer.setPosition(0);
			while( !pointer.isEOF() ) {
				// read in first header
				InputUtility.readComplete(input, short_buffer);
				name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, short_buffer);
				type_name_length = TransformValue2.toShort(short_buffer);
				InputUtility.readComplete(input, int_buffer);
				value_length = TransformValue2.toInteger(int_buffer);
				name = new String(InputUtility.readComplete(input, new byte[name_length]));
				// is it the searched for attribute?
				if( !attribute_name.equals(name) ) {
					// no: skip bytes and continue searching
					pointer.changePosition(type_name_length + value_length);
					continue;
				}
				position_of_attribute = pointer.getPositionInFlake() - name_length
						- int_buffer.length - 2 * short_buffer.length;
				length_of_attribute = int_buffer.length + 2 * short_buffer.length + name_length
						+ type_name_length + value_length;
				break;
			}
			if( position_of_attribute < 0 ) {
				throw new IllegalArgumentException("The attribute_flake does not contain the attribute \""
						+ attribute_name + "\"!");
			}
			attribute_flake.cutAt(position_of_attribute, length_of_attribute);
		}
		catch( Exception e ) {
			throw new StorageException("Could remove the attribute \"" + name + "\"!", e);
		}
	}
	
}
