package com.paypal.hera.util;

import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/*To better understand NetString, users of this file are asked to refer to
 * TODO: http://dev.paypal.com/wiki/JavaInfrastructure/NetstringUtility
 */
/**
 * <p>NetStringObj class.</p>
 */
public final class NetStringObj {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	private final long command;
    private final byte[] data;

    /**
     * <p>Constructor for NetStringObj.</p>
     *
     * @param is a {@link java.io.InputStream} object.
     * @throws java.io.IOException if any.
     */
    public NetStringObj(InputStream is) throws IOException {
    	NetStringObj ns = readFromInputStream(is);
    	this.command = ns.command;
    	this.data = ns.data;
    }

    /**
     * <p>Constructor for NetStringObj.</p>
     *
     * @param command a long.
     * @param nsobs an array of {@link com.paypal.infra.util.netstring.NetStringObj} objects.
     * @throws java.io.IOException if any.
     */
    public NetStringObj(long command, NetStringObj[] nsobs) throws IOException {
    	this(command, toByteArray(nsobs), false);
    }

    /**
     * <p>Constructor for NetStringObj.</p>
     *
     * @param command a long.
     * @param nsdata an array of byte.
     */
    public NetStringObj(long command, byte[] nsdata) {
       this(command, nsdata, true);
    }

    /**
     * <p>Constructor for NetStringObj.</p>
     *
     * @param command a long.
     * @param nsdata a {@link java.lang.String} object.
     */
    public NetStringObj(long command, String nsdata) {
        this(command, (nsdata!=null)?nsdata.getBytes():null, false);
    }

    private NetStringObj(long command, byte[] nsdata, boolean copyData) {
        this.command = command;
        if ((nsdata == null) || (nsdata.length == 0)) {
        	this.data = EMPTY_BYTE_ARRAY;
        }
        else if (copyData) {
        	this.data = java.util.Arrays.copyOf(nsdata, nsdata.length); //pkrastogi - this array copy makes sense, because we want to isolate our copy from the app
        }
        else {
        	this.data = nsdata;
        }
    }

	private static byte[] toByteArray(NetStringObj[] nsobs) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (NetStringObj n : nsobs) {
            n.writeToOutputStream(baos);
        }

       return baos.toByteArray();
	}

    private static int getNSLength(InputStream is, byte delimiter) throws IOException {
        int nslength = 0;
        int b = is.read();
        // Read from the stream till we receive a CHARACTER_COLON so that
        // we can calculate the length of the netstring.
        // Once we know the length of the netstring we read that
        // many bytes from the socket stream.
        while (b != delimiter && b != -1) {
            // I removed the (b != 0) check here, because that implies we
            // have embedded nulls in the b, which should be
            // Disallowed. If we have to support this for some
            // weird backwards compatible thing we should add it back in.
        	// validating whether a numeric stream of digits is actually numeric
        	final int digit = b - NetStringConstants.CHARACTER_ZERO;
        	if (digit < 0 || digit >= 10) {
                  throw new IOException("Invalid character in stream: expected numeric. ByteValue = " + b);
            }
            nslength = nslength * 10 + digit;

            b = is.read();
        }

        if (b == -1) {
            throw new IOException("Unexpected end of stream");
        }
        return nslength;
    }


    private static StreamResult getNSCommand(InputStream is, int maxStreamLengthLength) throws IOException {
        return readLimitedStream(is, maxStreamLengthLength, NetStringConstants.CHARACTER_SPACE);
    }

    // given an input stream read until we hit the delimiter, and
    // return the number found. Throws IOException on any errors.
    // Passing in the delimiter allows us to use this to read nslength
    // as well as the command. Where the delimiter for length is ':' (colon)
    // and for command is ' ' (space).
    private static StreamResult readLimitedStream(InputStream is, int bytesRemaining, byte delimiter) throws IOException {
    	StreamResult result = new StreamResult();
        if (bytesRemaining <= 0) {
        	return result;
        }

    	int b = is.read();
        if (b >= 0) {
        	result.m_bytesRead++;
        	bytesRemaining--;
        }

        // Read from the stream till we receive a CHARACTER_COLON so that
        // we can calculate the length of the netstring.
        // Once we know the length of the netstring we read that
        // many bytes from the socket stream.
        while (b != delimiter && (bytesRemaining >= 0)) {
            // I removed the (b != 0) check here, because that implies we
            // have embedded nulls in the b, which should be
            // Disallowed. If we have to support this for some
            // weird backwards compatible thing we should add it back in.
        	// validating whether a numeric stream of digits is actually numeric
        	final int digit = b - NetStringConstants.CHARACTER_ZERO;
        	if (digit < 0 || digit >= 10) {
                  throw new IOException("Invalid character in stream: expected numeric. ByteValue = " + b);
            }
        	result.m_result = result.m_result * 10 + digit;

        	// if check not to read Comma at last
        	if (bytesRemaining == 0)
        	{
        		break;
        	}

            b = is.read();
            if (b < 0) {
            	break;
            }
            result.m_bytesRead++;
            bytesRemaining--;
        }

        if ((b<0) && (bytesRemaining > 0)) {
            throw new IOException("Unexpected end of stream");
        }
        return result;
    }

    // this should be a utility function somewhere else.
    private byte[] readXBytes(InputStream is, int bytesRemaining) throws IOException {
    	if (bytesRemaining <= 0) {
        	return EMPTY_BYTE_ARRAY;
        }

    	byte[] bytesRead = new byte[bytesRemaining];
        int currentLength = 0;

        while (currentLength < bytesRemaining) {
        	int b = is.read(bytesRead, currentLength, bytesRemaining - currentLength);
            if (b < 0) {
            	throw new IOException("Unexpected end of stream");
            }
        	currentLength += b;
        }
        return bytesRead;
    }

    /**
     *
     * @param is the InputStream to read from.
     *
     * @ returns the bytes read from the input stream
     */
    private NetStringObj readFromInputStream(InputStream is) throws IOException {
    	int nsLength = getNSLength(is, NetStringConstants.CHARACTER_COLON);

        StreamResult commandRecord = getNSCommand(is, nsLength);
        final long local_command = commandRecord.getResult();
        int nsCommandBytesRead = commandRecord.getBytesRead();

        // -1 because data should not contain comma
        final byte[] local_data = readXBytes(is, nsLength - nsCommandBytesRead); // this gets the data

        int comma = is.read();
        if (comma != NetStringConstants.CHARACTER_COMMA) {
            throw new IOException("Netstring object not properly terminated");
        }

        return new NetStringObj(local_command, local_data, false);
    }


    /**
     * <p>writeToOutputStream.</p>
     *
     * @param os a {@link java.io.OutputStream} object.
     * @throws java.io.IOException if any.
     */
    public void writeToOutputStream(OutputStream os) throws IOException{
        // String sCommand = this.command.toString("UTF-8");
    	String sCommand = String.valueOf(this.command);
        long length = sCommand.length();
        if (this.data.length > 0) {
        	length += 1 + this.data.length;
        }
        // write the length
        os.write(String.valueOf(length).getBytes("UTF-8"));
        os.write(NetStringConstants.CHARACTER_COLON);


        // write the command
        os.write(sCommand.getBytes("UTF-8"));

        //only write the data payload if there is data
        if (this.data.length > 0) {
	        //write data separator
	        os.write(NetStringConstants.CHARACTER_SPACE);

	        // write the data
	        os.write(data);
        }

        // finish the netstring
        os.write(NetStringConstants.CHARACTER_COMMA);
    }

    /**
     * <p>Getter for the field <code>command</code>.</p>
     *
     * @return a long.
     */
    public long getCommand() {
    	return this.command;
    }

    /**
     * <p>Getter for the field <code>data</code>.</p>
     *
     * @return an array of byte.
     */
    public byte[] getData() {
        /* Copied data array to a new array to prevent the 'exposure of internal representation by returning
         * reference to mutable object' */
    	//pkrastogi - removing unnecessary array copy.  In the end if the app modifies this data it doesn't affect anything.
        return data;
    }

    /**
     * Some NetString objects' data element is itself a netstring object. This is a convenience function
     * to return the data as an array of netstring objects.
     *
     * @return an array of {@link com.paypal.infra.util.netstring.NetStringObj} objects.
     * @throws java.io.IOException if any.
     */
    public NetStringObj[] getDataAsNetStringObjects() throws IOException{
        ByteArrayInputStream bais = new ByteArrayInputStream(this.data);
        ArrayList<NetStringObj> nsobjs = new ArrayList<NetStringObj>();
        while (bais.available() > 0) {
            NetStringObj nso = new NetStringObj(bais);
            nsobjs.add(nso);
        }
        NetStringObj[] returnArray = new NetStringObj[nsobjs.size()];
        return nsobjs.toArray(returnArray);
    }

    static class StreamResult
    {
		long m_result = 0;
		int m_bytesRead = 0;

		long getResult() {
			return m_result;
		}

		int getBytesRead() {
			return m_bytesRead;
		}
    }
}
