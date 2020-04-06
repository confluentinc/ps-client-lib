/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package io.confluent.clientmetrics.avro;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class MetricValueAVRO extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2645798474034335907L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MetricValueAVRO\",\"namespace\":\"io.confluent.internalmetrics.avro\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<MetricValueAVRO> ENCODER =
      new BinaryMessageEncoder<MetricValueAVRO>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<MetricValueAVRO> DECODER =
      new BinaryMessageDecoder<MetricValueAVRO>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<MetricValueAVRO> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<MetricValueAVRO> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<MetricValueAVRO> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<MetricValueAVRO>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this MetricValueAVRO to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a MetricValueAVRO from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a MetricValueAVRO instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static MetricValueAVRO fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public java.lang.CharSequence value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public MetricValueAVRO() {}

  /**
   * All-args constructor.
   * @param value The new value for value
   */
  public MetricValueAVRO(java.lang.CharSequence value) {
    this.value = value;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: value = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public java.lang.CharSequence getValue() {
    return value;
  }


  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(java.lang.CharSequence value) {
    this.value = value;
  }

  /**
   * Creates a new MetricValueAVRO RecordBuilder.
   * @return A new MetricValueAVRO RecordBuilder
   */
  public static io.confluent.clientmetrics.avro.MetricValueAVRO.Builder newBuilder() {
    return new io.confluent.clientmetrics.avro.MetricValueAVRO.Builder();
  }

  /**
   * Creates a new MetricValueAVRO RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new MetricValueAVRO RecordBuilder
   */
  public static io.confluent.clientmetrics.avro.MetricValueAVRO.Builder newBuilder(io.confluent.clientmetrics.avro.MetricValueAVRO.Builder other) {
    if (other == null) {
      return new io.confluent.clientmetrics.avro.MetricValueAVRO.Builder();
    } else {
      return new io.confluent.clientmetrics.avro.MetricValueAVRO.Builder(other);
    }
  }

  /**
   * Creates a new MetricValueAVRO RecordBuilder by copying an existing MetricValueAVRO instance.
   * @param other The existing instance to copy.
   * @return A new MetricValueAVRO RecordBuilder
   */
  public static io.confluent.clientmetrics.avro.MetricValueAVRO.Builder newBuilder(io.confluent.clientmetrics.avro.MetricValueAVRO other) {
    if (other == null) {
      return new io.confluent.clientmetrics.avro.MetricValueAVRO.Builder();
    } else {
      return new io.confluent.clientmetrics.avro.MetricValueAVRO.Builder(other);
    }
  }

  /**
   * RecordBuilder for MetricValueAVRO instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MetricValueAVRO>
    implements org.apache.avro.data.RecordBuilder<MetricValueAVRO> {

    private java.lang.CharSequence value;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(io.confluent.clientmetrics.avro.MetricValueAVRO.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.value)) {
        this.value = data().deepCopy(fields()[0].schema(), other.value);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
    }

    /**
     * Creates a Builder by copying an existing MetricValueAVRO instance
     * @param other The existing instance to copy.
     */
    private Builder(io.confluent.clientmetrics.avro.MetricValueAVRO other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.value)) {
        this.value = data().deepCopy(fields()[0].schema(), other.value);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public java.lang.CharSequence getValue() {
      return value;
    }


    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public io.confluent.clientmetrics.avro.MetricValueAVRO.Builder setValue(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.value = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public io.confluent.clientmetrics.avro.MetricValueAVRO.Builder clearValue() {
      value = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MetricValueAVRO build() {
      try {
        MetricValueAVRO record = new MetricValueAVRO();
        record.value = fieldSetFlags()[0] ? this.value : (java.lang.CharSequence) defaultValue(fields()[0]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<MetricValueAVRO>
    WRITER$ = (org.apache.avro.io.DatumWriter<MetricValueAVRO>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<MetricValueAVRO>
    READER$ = (org.apache.avro.io.DatumReader<MetricValueAVRO>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.value);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.value = in.readString(this.value instanceof Utf8 ? (Utf8)this.value : null);

    } else {
      for (int i = 0; i < 1; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.value = in.readString(this.value instanceof Utf8 ? (Utf8)this.value : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










