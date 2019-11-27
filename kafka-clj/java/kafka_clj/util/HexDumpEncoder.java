/*
 * Copyright 2015 Erik Wramner
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kafka_clj.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Shamelessly copied from: https://raw.githubusercontent.com/erik-wramner/HexDumpEncoder/master/src/main/java/name/wramner/util/HexDumpEncoder.java
 *
 * This is a drop-in replacement for Sun's internal <code>sun.misc.HexDumpEncoder</code> class. It should be completely
 * compatible with the output from Sun's encoder, making it possible to get rid of the now forbidden reference to the
 * internal class without affecting the application's behavior in any way.
 *
 * @author Erik Wramner
 */
public class HexDumpEncoder {
    private static final int BYTES_PER_LINE = 16;
    private static final int BYTES_PER_GROUP = 8;
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public String encodeBuffer(byte[] buf) {
        try {
            Appendable sb = new StringBuilder();
            internalEncode(buf, buf.length, 0, sb, false);
            return sb.toString();
        } catch (IOException e) {
            // This should never happen
            throw new IllegalStateException("IO exception from string builder", e);
        }
    }

    public void encodeBuffer(byte[] buf, OutputStream out) throws IOException {
        internalEncode(buf, buf.length, 0, new OutputStreamAppendableAdapter(out), false);
    }

    public void encodeBuffer(ByteBuffer byteBuffer, OutputStream out) throws IOException {
        OutputStreamAppendableAdapter appendable = new OutputStreamAppendableAdapter(out);
        internalEncode(byteBuffer, appendable, false);
    }

    public String encodeBuffer(ByteBuffer byteBuffer) {
        StringBuilder sb = new StringBuilder();
        try {
            internalEncode(byteBuffer, sb, false);
        } catch (IOException e) {
            // This should never happen
            throw new IllegalStateException("IO exception from string builder", e);
        }
        return sb.toString();
    }

    public void encodeBuffer(InputStream in, OutputStream out) throws IOException {
        Appendable outAppendable = new OutputStreamAppendableAdapter(out);
        int address = 0;
        byte[] buf = new byte[BYTES_PER_LINE * 100];
        int bytesRead = fillBuffer(in, buf);
        while (bytesRead > 0) {
            internalEncode(buf, bytesRead, address, outAppendable, false);
            if (bytesRead == buf.length) {
                address += bytesRead;
                bytesRead = fillBuffer(in, buf);
            } else {
                bytesRead = 0;
            }
        }
    }

    public String encode(byte[] buf) {
        try {
            Appendable sb = new StringBuilder();
            internalEncode(buf, buf.length, 0, sb, true);
            return sb.toString();
        } catch (IOException e) {
            // This should never happen
            throw new IllegalStateException("IO exception from string builder", e);
        }
    }

    public void encode(byte[] buf, OutputStream out) throws IOException {
        internalEncode(buf, buf.length, 0, new OutputStreamAppendableAdapter(out), true);
    }

    public void encode(ByteBuffer byteBuffer, OutputStream out) throws IOException {
        OutputStreamAppendableAdapter appendable = new OutputStreamAppendableAdapter(out);
        internalEncode(byteBuffer, appendable, true);
    }

    public String encode(ByteBuffer byteBuffer) {
        StringBuilder sb = new StringBuilder();
        try {
            internalEncode(byteBuffer, sb, true);
        } catch (IOException e) {
            // This should never happen
            throw new IllegalStateException("IO exception from string builder", e);
        }
        return sb.toString();
    }

    public void encode(InputStream in, OutputStream out) throws IOException {
        Appendable outAppendable = new OutputStreamAppendableAdapter(out);
        int address = 0;
        byte[] buf = new byte[BYTES_PER_LINE * 100];
        int bytesRead = fillBuffer(in, buf);
        while (bytesRead > 0) {
            internalEncode(buf, bytesRead, address, outAppendable, true);
            if (bytesRead == buf.length) {
                address += bytesRead;
                bytesRead = fillBuffer(in, buf);
            } else {
                bytesRead = 0;
            }
        }
    }

    private int fillBuffer(InputStream in, byte[] buf) throws IOException {
        int totalBytesRead = 0;
        int bytesLeft = buf.length;
        while (bytesLeft > 0) {
            int bytesRead = in.read(buf, totalBytesRead, bytesLeft);
            if (bytesRead == -1) {
                break;
            } else {
                bytesLeft -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        return totalBytesRead;
    }

    private void internalEncode(ByteBuffer byteBuffer, Appendable appendable, boolean dropAsciiForPartialLine)
            throws IOException {
        int address = 0;
        while (byteBuffer.hasRemaining()) {
            byte[] buf = new byte[Math.min(byteBuffer.remaining(), BYTES_PER_LINE * 100)];
            byteBuffer.get(buf);
            internalEncode(buf, buf.length, address, appendable, dropAsciiForPartialLine);
            address += buf.length;
        }
    }

    private void internalEncode(byte[] buf, int length, int baseAddress, Appendable sb, boolean dropAsciiForPartialLine)
            throws IOException {
        StringBuilder asciiStringBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (i % BYTES_PER_LINE == 0) {
                if (asciiStringBuilder.length() > 0) {
                    sb.append("  ");
                    sb.append(asciiStringBuilder);
                    sb.append(LINE_SEPARATOR);
                    asciiStringBuilder.setLength(0);
                }
                appendHexDigit(sb, (byte) (((baseAddress + i) >>> 8) & 0xFF));
                appendHexDigit(sb, (byte) ((baseAddress + i) & 0xFF));
                sb.append(':');
            } else if (i % BYTES_PER_GROUP == 0) {
                sb.append("  ");
            }
            sb.append(' ');
            final byte b = buf[i];
            appendHexDigit(sb, b);
            if (b >= 0x20 && b <= 0x7a) {
                asciiStringBuilder.append((char) b);
            } else {
                asciiStringBuilder.append('.');
            }
        }

        if (dropAsciiForPartialLine) {
            // Be bug compatible
            if (asciiStringBuilder.length() > 0) {
                sb.append(' ');
                if (length % BYTES_PER_LINE == BYTES_PER_GROUP) {
                    sb.append("  ");
                } else if (length % BYTES_PER_LINE == 0) {
                    sb.append(' ');
                    sb.append(asciiStringBuilder);
                    sb.append(LINE_SEPARATOR);
                }
            }
        } else if (asciiStringBuilder.length() > 0) {
            int remainingBytes = BYTES_PER_LINE - length % BYTES_PER_LINE;
            if (remainingBytes != BYTES_PER_LINE) {
                for (int i = 0; i < remainingBytes; i++) {
                    sb.append("   ");
                }
                if (remainingBytes >= BYTES_PER_GROUP) {
                    sb.append("  ");
                }
            }
            sb.append("  ");
            sb.append(asciiStringBuilder);
            sb.append(LINE_SEPARATOR);
        }
    }

    private void appendHexDigit(Appendable sb, byte b) throws IOException {
        int nibble = (char) (b >> 4 & 0xF);
        sb.append((char) (nibble > 9 ? nibble - 10 + 65 : nibble + 48));
        nibble = (char) (b & 0xF);
        sb.append((char) (nibble > 9 ? nibble - 10 + 65 : nibble + 48));
    }

    private static class OutputStreamAppendableAdapter implements Appendable {
        private final OutputStream _out;

        public OutputStreamAppendableAdapter(OutputStream out) {
            _out = out;
        }

        @Override
        public Appendable append(CharSequence cs) throws IOException {
            append(cs, 0, cs.length());
            return this;
        }

        @Override
        public Appendable append(CharSequence cs, int start, int end) throws IOException {
            for (int i = start; i < end; i++) {
                append(cs.charAt(i));
            }
            return this;
        }

        @Override
        public Appendable append(char c) throws IOException {
            _out.write(c);
            return this;
        }
    }
}
