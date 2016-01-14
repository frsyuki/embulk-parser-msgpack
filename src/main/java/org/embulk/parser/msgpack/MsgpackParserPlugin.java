package org.embulk.parser.msgpack;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;
import java.io.IOException;
import java.io.EOFException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.value.ValueType;
import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.util.Timestamps;
import org.embulk.spi.util.DynamicPageBuilder;
import org.embulk.spi.util.DynamicColumnSetter;
import org.embulk.spi.util.DynamicColumnSetterFactory;
import org.embulk.spi.util.dynamic.BooleanColumnSetter;
import org.embulk.spi.util.dynamic.LongColumnSetter;
import org.embulk.spi.util.dynamic.DoubleColumnSetter;
import org.embulk.spi.util.dynamic.StringColumnSetter;
import org.embulk.spi.util.dynamic.TimestampColumnSetter;
import org.embulk.spi.util.dynamic.JsonColumnSetter;
import org.embulk.spi.util.dynamic.DefaultValueSetter;
import org.embulk.spi.util.dynamic.NullDefaultValueSetter;

public class MsgpackParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("file_encoding")
        @ConfigDefault("\"sequence\"")
        public FileEncoding getFileEncoding();

        @Config("row_encoding")
        @ConfigDefault("\"map\"")
        public RowEncoding getRowEncoding();

        @Config("columns")
        public SchemaConfig getSchemaConfig();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    public static enum FileEncoding
    {
        SEQUENCE("sequence"),
        ARRAY("array");

        private final String name;

        private FileEncoding(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static FileEncoding of(String name)
        {
            for (FileEncoding enc : FileEncoding.values()) {
                if (enc.toString().equals(name)) {
                    return enc;
                }
            }
            throw new ConfigException(String.format("Invalid FileEncoding '%s'. Available options are sequence or array", name));
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name;
        }
    }

    public static enum RowEncoding
    {
        ARRAY("array"),
        MAP("map");

        private final String name;

        private RowEncoding(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static RowEncoding of(String name)
        {
            for (RowEncoding enc : RowEncoding.values()) {
                if (enc.toString().equals(name)) {
                    return enc;
                }
            }
            if ("object".equals(name)) {
                // alias of map
                return MAP;
            }
            throw new ConfigException(String.format("Invalid RowEncoding '%s'. Available options are array or map", name));
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name;
        }
    }

    public interface PluginTaskFormatter
            extends Task, TimestampFormatter.Task
    { }

    private interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    { }

    private static class FileInputMessageBufferInput
            implements MessageBufferInput
    {
        private final FileInput input;
        private Buffer lastBuffer = null;

        public FileInputMessageBufferInput(FileInput input)
        {
            this.input = input;
        }

        @Override
        public MessageBuffer next()
        {
            Buffer b = input.poll();
            if (lastBuffer != null) {
                lastBuffer.release();
            }
            lastBuffer = b;
            if (b == null) {
                return null;
            }
            else {
                return MessageBuffer.wrap(b.array()).slice(b.offset(), b.limit());
            }
        }

        @Override
        public void close()
        {
            if (lastBuffer != null) {
                lastBuffer.release();
            }
            input.close();
        }
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        control.run(task.dump(), task.getSchemaConfig().toSchema());
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        RowEncoding rowEncoding = task.getRowEncoding();
        FileEncoding fileEncoding = task.getFileEncoding();

        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputMessageBufferInput(input));
                PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output)) {

            TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());
            Map<Column, DynamicColumnSetter> setters = newColumnSetters(pageBuilder,
                    task.getSchemaConfig(), timestampParsers, taskSource.loadTask(PluginTaskFormatter.class));

            RowReader reader;
            switch (rowEncoding) {
            case ARRAY:
                reader = new ArrayRowReader(setters);
                break;
            case MAP:
                reader = new MapRowReader(setters);
                break;
            default:
                throw new IllegalArgumentException("Unexpected row encoding");
            }

            while (input.nextFile()) {
                switch (fileEncoding) {
                case SEQUENCE:
                    // do nothing
                    break;
                case ARRAY:
                    // skip array header to convert array to sequence
                    unpacker.unpackArrayHeader();
                    break;
                }

                while (reader.next(unpacker)) {
                    pageBuilder.addRecord();
                }
            }

            pageBuilder.finish();

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<Column, DynamicColumnSetter> newColumnSetters(PageBuilder pageBuilder,
            SchemaConfig schema, TimestampParser[] timestampParsers, TimestampFormatter.Task formatterTask)
    {
        ImmutableMap.Builder<Column, DynamicColumnSetter> builder = ImmutableMap.builder();
        int index = 0;
        for (ColumnConfig c : schema.getColumns()) {
            Column column = c.toColumn(index);
            Type type = column.getType();

            DefaultValueSetter defaultValue = new NullDefaultValueSetter();
            DynamicColumnSetter setter;

            if (type instanceof BooleanType) {
                setter = new BooleanColumnSetter(pageBuilder, column, defaultValue);

            } else if (type instanceof LongType) {
                setter = new LongColumnSetter(pageBuilder, column, defaultValue);

            } else if (type instanceof DoubleType) {
                setter = new DoubleColumnSetter(pageBuilder, column, defaultValue);

            } else if (type instanceof StringType) {
                TimestampFormatter formatter = new TimestampFormatter(formatterTask,
                        Optional.of(c.getOption().loadConfig(TimestampColumnOption.class)));
                setter = new StringColumnSetter(pageBuilder, column, defaultValue, formatter);

            } else if (type instanceof TimestampType) {
                // TODO use flexible time format like Ruby's Time.parse
                TimestampParser parser = timestampParsers[column.getIndex()];
                setter = new TimestampColumnSetter(pageBuilder, column, defaultValue, parser);

            } else if (type instanceof JsonType) {
                TimestampFormatter formatter = new TimestampFormatter(formatterTask,
                        Optional.of(c.getOption().loadConfig(TimestampColumnOption.class)));
                setter = new JsonColumnSetter(pageBuilder, column, defaultValue, formatter);

            } else {
                throw new ConfigException("Unknown column type: "+type);
            }

            builder.put(column, setter);
            index++;
        }
        return builder.build();
    }

    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);

    static void unpackToSetter(MessageUnpacker unpacker, DynamicColumnSetter setter)
            throws IOException
    {
        MessageFormat format = unpacker.getNextFormat();
        switch (format.getValueType()) {
        case NIL:
            unpacker.unpackNil();
            setter.setNull();
            break;

        case BOOLEAN:
            setter.set(unpacker.unpackBoolean());
            break;

        case INTEGER:
            if (format == MessageFormat.UINT64) {
                BigInteger bi = unpacker.unpackBigInteger();
                if (0 <= bi.compareTo(LONG_MIN) && bi.compareTo(LONG_MAX) <= 0) {
                    setter.set(bi.longValue());
                } else {
                    setter.setNull();  // TODO set default value
                }
            } else {
                setter.set(unpacker.unpackLong());
            }
            break;

        case FLOAT:
            setter.set(unpacker.unpackDouble());
            break;

        case STRING:
            setter.set(unpacker.unpackString());
            break;

        case BINARY:
            setter.set(unpacker.unpackString());
            break;

        case ARRAY:
        case MAP:
            // TODO embulk 0.8.1 will add set(Value) api to DynamicColumnSetter
            ((org.embulk.spi.util.dynamic.AbstractDynamicColumnSetter) setter).set(unpacker.unpackValue());
            break;

        case EXTENSION:
            unpacker.skipValue();
            setter.setNull();
            break;
        }
    }

    private interface RowReader
    {
        public boolean next(MessageUnpacker unpacker) throws IOException;
    }

    private class ArrayRowReader
            implements RowReader
    {
        private final DynamicColumnSetter[] columnSetters;

        public ArrayRowReader(Map<Column, DynamicColumnSetter> setters)
        {
            this.columnSetters = new DynamicColumnSetter[setters.size()];
            for (Map.Entry<Column, DynamicColumnSetter> pair : setters.entrySet()) {
                columnSetters[pair.getKey().getIndex()] = pair.getValue();
            }
        }

        public boolean next(MessageUnpacker unpacker) throws IOException
        {
            int n;
            try {
                n = unpacker.unpackArrayHeader();
            } catch (MessageInsufficientBufferException ex) {
                // TODO EOFException?
                return false;
            }
            for (int i = 0; i < n; i++) {
                if (i < columnSetters.length) {
                    unpackToSetter(unpacker, columnSetters[i]);
                } else {
                    unpacker.skipValue();
                }
            }
            return true;
        }
    }

    private class MapRowReader
            implements RowReader
    {
        private final Map<String, DynamicColumnSetter> columnSetters;

        public MapRowReader(Map<Column, DynamicColumnSetter> setters)
        {
            this.columnSetters = new TreeMap<>();
            for (Map.Entry<Column, DynamicColumnSetter> pair : setters.entrySet()) {
                columnSetters.put(pair.getKey().getName(), pair.getValue());
            }
        }

        public boolean next(MessageUnpacker unpacker) throws IOException
        {
            int n;
            try {
                n = unpacker.unpackMapHeader();
            } catch (MessageInsufficientBufferException ex) {
                // TODO EOFException?
                return false;
            }
            for (int i = 0; i < n; i++) {
                MessageFormat format = unpacker.getNextFormat();
                if (!format.getValueType().isRawType()) {
                    unpacker.skipValue();
                    continue;
                }
                // TODO optimize
                //MessageBuffer key = unpacker.readPayloadAsReference(unpacker.unpackRawStringHeader());
                String key = new String(unpacker.readPayload(unpacker.unpackRawStringHeader()));
                DynamicColumnSetter setter = columnSetters.get(key);
                if (setter != null) {
                    unpackToSetter(unpacker, setter);
                } else {
                    unpacker.skipValue();
                }
            }
            return true;
        }
    }

    private static class MessageBufferEqualComparator
            implements Comparator<MessageBuffer>
    {
        @Override
        public int compare(MessageBuffer o1, MessageBuffer o2)
        {
            if (o1.size() == o2.size()) {
                int offset = 0;
                int length = o1.size();
                while (length - offset > 8) {
                    long a = o1.getLong(offset);
                    long b = o2.getLong(offset);
                    if (a != b) {
                        return (int) (a - b);
                    }
                    offset += 8;
                }
                while (length - offset > 0) {
                    byte a = o1.getByte(offset);
                    byte b = o2.getByte(offset);
                    if (a != b) {
                        return a - b;
                    }
                    offset += 1;
                }
                return 0;
            } else {
                return o1.size() - o2.size();
            }
        }
    }
}
