/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.highlevel.internal.KeyExtractor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.iq80.snappy.Snappy;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static java.nio.charset.StandardCharsets.UTF_8;

public enum MessageUtil {
  ;

  public static final int HEADER_SIZE = 24;

  public static final byte MAGIC_INT = (byte) 0x79;
  public static final byte MAGIC_REQ = (byte) 0x80;
  public static final byte MAGIC_RES = (byte) 0x81;

  // Duplex mode, server-initiated request and client response
  public static final byte MAGIC_SERVER_REQ = (byte) 0x82;
  public static final byte MAGIC_SERVER_RES = (byte) 0x83;

  public static final short KEY_LENGTH_OFFSET = 2;
  public static final short EXTRAS_LENGTH_OFFSET = 4;
  public static final short DATA_TYPE_OFFSET = 5;
  public static final short VBUCKET_OFFSET = 6;
  public static final short BODY_LENGTH_OFFSET = 8;
  public static final short OPAQUE_OFFSET = 12;
  public static final short CAS_OFFSET = 16;

  public static final byte NOOP_OPCODE = 0x0a; // ordinary client-initiated noop, not to be confused with DCP_NOOP
  public static final byte VERSION_OPCODE = 0x0b;
  public static final byte HELLO_OPCODE = 0x1f;
  public static final byte SASL_LIST_MECHS_OPCODE = 0x20;
  public static final byte SASL_AUTH_OPCODE = 0x21;
  public static final byte SASL_STEP_OPCODE = 0x22;
  public static final byte GET_SEQNOS_OPCODE = 0x48;
  public static final byte OPEN_CONNECTION_OPCODE = 0x50;
  public static final byte DCP_ADD_STREAM_OPCODE = 0x51;
  public static final byte DCP_STREAM_CLOSE_OPCODE = 0x52;
  public static final byte DCP_STREAM_REQUEST_OPCODE = 0x53;
  public static final byte DCP_FAILOVER_LOG_OPCODE = 0x54;
  public static final byte DCP_STREAM_END_OPCODE = 0x55;
  public static final byte DCP_SNAPSHOT_MARKER_OPCODE = 0x56;
  public static final byte DCP_MUTATION_OPCODE = 0x57;
  public static final byte DCP_DELETION_OPCODE = 0x58;
  public static final byte DCP_EXPIRATION_OPCODE = 0x59;
  public static final byte DCP_FLUSH_OPCODE = 0x5a;
  public static final byte DCP_SET_VBUCKET_STATE_OPCODE = 0x5b;
  public static final byte DCP_NOOP_OPCODE = 0x5c;
  public static final byte DCP_BUFFER_ACK_OPCODE = 0x5d;
  public static final byte DCP_CONTROL_OPCODE = 0x5e;
  public static final byte DCP_SYSTEM_EVENT_OPCODE = 0x5f;
  public static final byte DCP_SEQNO_ADVANCED_OPCODE = 0x64;
  public static final byte DCP_OSO_SNAPSHOT_MARKER_OPCODE = 0x65;
  public static final byte SELECT_BUCKET_OPCODE = (byte) 0x89;
  public static final byte OBSERVE_SEQNO_OPCODE = (byte) 0x91;
  public static final byte GET_CLUSTER_CONFIG_OPCODE = (byte) 0xb5;
  public static final byte GET_COLLECTIONS_MANIFEST_OPCODE = (byte) 0xba;

  public static final byte INTERNAL_ROLLBACK_OPCODE = 0x00;

  // Duplex mode, server-initiated commands
  public static final byte CLUSTERMAP_CHANGE_NOTIFICATION_OPCODE = 0x01;
  public static final byte AUTHENTICATE_OPCODE = 0x02;
  public static final byte ACTIVE_EXTERNAL_USERS_OPCODE = 0x03;

  private static final String[] OPCODE_NAMES = initOpcodeNames();
  private static final String[] FORMATTED_OPCODE_NAMES = initFormattedOpcodeNames();


  private static String[] initOpcodeNames() {
    final String[] names = new String[256];
    try {
      final String suffix = "_OPCODE";
      for (Field f : MessageUtil.class.getDeclaredFields()) {
        if (f.getType() == Byte.TYPE
            && Modifier.isPublic(f.getModifiers())
            && Modifier.isStatic(f.getModifiers())
            && f.getName().endsWith(suffix)) {
          final String nameWithoutSuffix = f.getName().substring(0, f.getName().length() - suffix.length());
          names[0xff & f.getByte(null)] = nameWithoutSuffix;
        }
      }
    } catch (Throwable ignore) {
      // Running in a restrictive environment? Oh well, opcode names are a luxury we can live without.
    }
    return names;
  }

  private static String[] initFormattedOpcodeNames() {
    final String[] names = new String[256];
    for (int i = 0; i < names.length; i++) {
      String name = OPCODE_NAMES[i];
      names[i] = name == null ? String.format("0x%02x", i) : name;
    }
    return names;
  }

  public static int getOpcode(ByteBuf buf) {
    return buf.getByte(1) & 0xff;
  }

  public static String getShortOpcodeName(int opcode) {
    try {
      return FORMATTED_OPCODE_NAMES[opcode];
    } catch (IndexOutOfBoundsException badRequestOrOpcodeArrayNotSizedCorrectly) {
      return String.valueOf(opcode);
    }
  }

  public static String getShortOpcodeName(ByteBuf buf) {
    return getShortOpcodeName(getOpcode(buf));
  }

  /**
   * Returns true if message can be processed and false if more data is needed.
   */
  public static boolean isComplete(final ByteBuf buffer) {
    int readable = buffer.readableBytes();
    if (readable < HEADER_SIZE) {
      return false;
    }
    return readable >= (HEADER_SIZE + buffer.getInt(BODY_LENGTH_OFFSET));
  }

  /**
   * Dumps the given ByteBuf in the "wire format".
   * <p>
   * Note that the response is undefined if a buffer with a different
   * content than the KV protocol is passed in.
   *
   * @return the String ready to be printed/logged.
   */
  public static String humanize(final ByteBuf buffer) {
    StringBuilder sb = new StringBuilder();

    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    int bodyLength = buffer.getInt(BODY_LENGTH_OFFSET);

    sb.append("Field          (offset) (value)\n-----------------------------------\n");
    sb.append(String.format("Magic          (0)      %s\n", formatMagic(buffer.getByte(0))));
    sb.append(String.format("Opcode         (1)      %s\n", formatOpcode(buffer.getByte(1))));
    sb.append(String.format("Key Length     (2,3)    0x%04x\n", keyLength));
    sb.append(String.format("Extras Length  (4)      0x%02x\n", extrasLength));
    sb.append(String.format("Data Type      (5)      0x%02x\n", buffer.getByte(5)));
    if (buffer.getByte(0) == MAGIC_REQ) {
      sb.append(String.format("VBucket        (6,7)    0x%04x\n", buffer.getShort(VBUCKET_OFFSET)));
    } else {
      sb.append(String.format("Status         (6,7)    %s\n", getResponseStatus(buffer)));
    }
    sb.append(String.format("Total Body     (8-11)   0x%08x\n", bodyLength));
    sb.append(String.format("Opaque         (12-15)  0x%08x\n", buffer.getInt(OPAQUE_OFFSET)));
    sb.append(String.format("CAS            (16-23)  0x%016x\n", buffer.getLong(CAS_OFFSET)));

    if (extrasLength > 0) {
      sb.append("+ Extras: \n" + ByteBufUtil.prettyHexDump(getExtras(buffer)) + "\n");
    } else {
      sb.append("No Extras\n");
    }
    if (keyLength > 0) {
      sb.append("+ Key: \n" + ByteBufUtil.prettyHexDump(getKey(buffer)) + "\n");
    } else {
      sb.append("No Key\n");
    }


    int contentLength = bodyLength - extrasLength - keyLength;
    if (contentLength > 0) {
      sb.append("+ Value: \n" + ByteBufUtil.prettyHexDump(getContent(buffer)) + "\n");
    } else {
      sb.append("No Value\n");
    }

    return sb.toString();
  }

  /**
   * Helper method to initialize a request with an opcode.
   */
  public static void initRequest(byte opcode, ByteBuf buffer) {
    buffer.writeByte(MessageUtil.MAGIC_REQ);
    buffer.writeByte(opcode);
    buffer.writeZero(HEADER_SIZE - 2);
  }

  /**
   * Helper method to initialize a response with an opcode.
   */
  public static void initResponse(byte opcode, ByteBuf buffer) {
    buffer.writeByte(MessageUtil.MAGIC_RES);
    buffer.writeByte(opcode);
    buffer.writeZero(HEADER_SIZE - 2);
  }

  public static void setExtras(ByteBuf extras, ByteBuf buffer) {
    byte oldExtrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    byte newExtrasLength = (byte) extras.readableBytes();
    int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
    int newBodyLength = oldBodyLength - oldExtrasLength + newExtrasLength;

    buffer.setByte(EXTRAS_LENGTH_OFFSET, newExtrasLength);
    buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

    buffer.setBytes(HEADER_SIZE, extras);
    buffer.writerIndex(HEADER_SIZE + newBodyLength);
  }

  public static ByteBuf getExtras(ByteBuf buffer) {
    return buffer.slice(HEADER_SIZE, buffer.getByte(EXTRAS_LENGTH_OFFSET));
  }

  public static void setVbucket(short vbucket, ByteBuf buffer) {
    buffer.setShort(VBUCKET_OFFSET, vbucket);
  }

  public static short getVbucket(ByteBuf buffer) {
    return buffer.getShort(VBUCKET_OFFSET);
  }

  /**
   * Helper method to set the key, update the key length and the content length.
   */
  public static void setKey(String key, ByteBuf buffer) {
    byte[] keyBytes = key.getBytes(UTF_8);
    short oldKeyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    short newKeyLength = (short) keyBytes.length;
    int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    int newBodyLength = oldBodyLength - oldKeyLength + newKeyLength;

    buffer.setShort(KEY_LENGTH_OFFSET, newKeyLength);
    buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

    buffer.writerIndex(HEADER_SIZE + extrasLength);
    buffer.writeBytes(keyBytes);
    buffer.writerIndex(HEADER_SIZE + newBodyLength);

    // todo: make sure stuff is still in order if content is there and its sliced in
    // todo: what if old key with different size is there
  }

  public static ByteBuf getKey(ByteBuf buffer) {
    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    return buffer.slice(HEADER_SIZE + extrasLength, keyLength);
  }

  public static String getKeyAsString(ByteBuf buffer) {
    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    return buffer.toString(HEADER_SIZE + extrasLength, keyLength, UTF_8);
  }

  public static CollectionIdAndKey getCollectionIdAndKey(ByteBuf buffer, boolean collectionsEnabled) {
    return (collectionsEnabled ? KeyExtractor.COLLECTIONS : KeyExtractor.NO_COLLECTIONS)
        .getCollectionIdAndKey(buffer);
  }

  /**
   * Sets the content payload of the buffer, updating the content length as well.
   */
  public static void setContent(ByteBuf content, ByteBuf buffer) {
    short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    int bodyLength = keyLength + extrasLength + content.readableBytes();
    int contentOffset = HEADER_SIZE + extrasLength + keyLength;

    buffer.setInt(BODY_LENGTH_OFFSET, bodyLength);
    buffer.writerIndex(contentOffset);
    buffer.ensureWritable(content.readableBytes());
    buffer.writeBytes(content);
    buffer.writerIndex(HEADER_SIZE + bodyLength);
  }

  public static void setContent(byte[] content, ByteBuf buffer) {
    setContent(Unpooled.wrappedBuffer(content), buffer);
  }

  /**
   * Returns the message content in its original form (possibly compressed).
   * <p>
   * The returned buffer shares its reference count with the given buffer.
   */
  public static ByteBuf getRawContent(ByteBuf buffer) {
    short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
    byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
    int contentLength = buffer.getInt(BODY_LENGTH_OFFSET) - keyLength - extrasLength;
    return buffer.slice(HEADER_SIZE + keyLength + extrasLength, contentLength);
  }

  /**
   * Returns the message content, uncompressed.
   * <p>
   * Callers need not release the returned buffer, since it either shares its
   * reference count with the given buffer, or is unpooled and not leak-aware.
   */
  public static ByteBuf getContent(ByteBuf buffer) {
    final ByteBuf rawContent = getRawContent(buffer);
    return isSnappyCompressed(buffer) ? Unpooled.wrappedBuffer(getContentAsByteArray(buffer)) : rawContent;
  }

  public static boolean isSnappyCompressed(ByteBuf buffer) {
    final byte DATA_TYPE_SNAPPY = 0x02;
    final byte dataType = buffer.getByte(DATA_TYPE_OFFSET);
    return (dataType & DATA_TYPE_SNAPPY) == DATA_TYPE_SNAPPY;
  }

  public static String getContentAsString(ByteBuf buffer) {
    return getContent(buffer).toString(UTF_8);
  }

  /**
   * Returns a new array containing the uncompressed content of the given message.
   */
  public static byte[] getContentAsByteArray(ByteBuf buffer) {
    final ByteBuf rawContent = getRawContent(buffer);

    if (!isSnappyCompressed(buffer)) {
      return ByteBufUtil.getBytes(rawContent);
    }

    if (rawContent.hasArray()) {
      return Snappy.uncompress(
          rawContent.array(), rawContent.arrayOffset() + rawContent.readerIndex(), rawContent.readableBytes());
    }

    final byte[] compressed = ByteBufUtil.getBytes(rawContent);
    return Snappy.uncompress(compressed, 0, compressed.length);
  }

  /**
   * @deprecated in favor of {@link #getResponseStatus(ByteBuf)}
   */
  @Deprecated
  public static short getStatus(ByteBuf buffer) {
    return buffer.getShort(VBUCKET_OFFSET);
  }

  public static ResponseStatus getResponseStatus(ByteBuf buffer) {
    try {
      return ResponseStatus.valueOf(buffer.getShort(VBUCKET_OFFSET));
    } catch (IndexOutOfBoundsException malformedResponse) {
      return ResponseStatus.MALFORMED_RESPONSE;
    }
  }

  public static boolean requiresFlowControlAck(ByteBuf message) {
    if (message.getByte(0) != MessageUtil.MAGIC_REQ) {
      return false;
    }

    switch (message.getByte(1)) {
      case DCP_MUTATION_OPCODE:
      case DCP_DELETION_OPCODE:
      case DCP_SNAPSHOT_MARKER_OPCODE:
      case DCP_EXPIRATION_OPCODE:
      case DCP_STREAM_END_OPCODE:
      case DCP_SET_VBUCKET_STATE_OPCODE:
        return true;
      default:
        return false;
    }
  }

  public static byte getDataType(ByteBuf buffer) {
    return buffer.getByte(DATA_TYPE_OFFSET);
  }

  public static void setDataType(byte dataType, ByteBuf buffer) {
    buffer.setByte(DATA_TYPE_OFFSET, dataType);
  }

  public static void setOpaque(int opaque, ByteBuf buffer) {
    buffer.setInt(OPAQUE_OFFSET, opaque);
  }

  public static int getOpaque(ByteBuf buffer) {
    return buffer.getInt(OPAQUE_OFFSET);
  }

  public static long getCas(ByteBuf buffer) {
    return buffer.getLong(CAS_OFFSET);
  }

  private static String formatOpcode(byte opcode) {
    return String.format("0x%02x (%s)", opcode, getOpcodeName(opcode));
  }

  /**
   * Returns the human-readable name for an opcode, or "?" if unrecognized.
   */
  private static String getOpcodeName(byte opcode) {
    final String unknownName = "?";
    try {
      final String name = OPCODE_NAMES[0xff & opcode];
      return name == null ? unknownName : name;
    } catch (IndexOutOfBoundsException opcodeArrayNotSizedCorrectly) {
      return unknownName;
    }
  }

  private static String formatMagic(byte magic) {
    final String name;
    switch (magic) {
      case MAGIC_REQ:
        name = "REQUEST";
        break;
      case MAGIC_RES:
        name = "RESPONSE";
        break;
      case MAGIC_SERVER_REQ:
        name = "SERVER REQUEST";
        break;
      case MAGIC_SERVER_RES:
        name = "SERVER RESPONSE";
        break;
      default:
        name = "?";
    }
    return String.format("0x%02x (%s)", magic, name);
  }
}
