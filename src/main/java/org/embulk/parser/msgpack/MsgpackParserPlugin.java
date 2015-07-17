package org.embulk.parser.msgpack;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;
import java.io.IOException;
import java.io.EOFException;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageUnpacker;
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
import org.embulk.spi.PageBuilder;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.util.Timestamps;
import org.embulk.spi.util.DynamicPageBuilder;
import org.embulk.spi.util.DynamicColumnSetter;
import org.embulk.spi.util.DynamicColumnSetterFactory;

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

    private static class FileInputMessageBufferInput
            implements MessageBufferInput
    {
        private final FileInput input;

        public FileInputMessageBufferInput(FileInput input)
        {
            this.input = input;
        }

        @Override
        public MessageBuffer next()
        {
            Buffer b = input.poll();
            return MessageBuffer.wrap(b.array()).slice(b.offset(), b.limit());
        }

        @Override
        public void close()
        {
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

        TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());
        Map<Column, DynamicColumnSetter> setters = newColumnSetters(task.getSchemaConfig(), timestampParsers);

        RowReader reader;
        switch (task.getRowEncoding()) {
        case ARRAY:
            reader = new ArrayRowReader(setters);
            break;
        case MAP:
            reader = new MapRowReader(setters);
            break;
        default:
            throw new IllegalArgumentException("Unexpected row encoding");
        }

        try (MessageUnpacker unpacker = new MessageUnpacker(new FileInputMessageBufferInput(input));
                PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output)) {
            switch (task.getFileEncoding()) {
            case SEQUENCE:
                // do nothing
                break;
            case ARRAY:
                // skip array header to convert array to sequence
                unpacker.unpackArrayHeader();
                break;
            }

            while (true) {
                boolean cont = reader.next(unpacker);
                if (!cont) {
                    break;
                }
                pageBuilder.addRecord();
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<Column, DynamicColumnSetter> newColumnSetters(SchemaConfig schema, TimestampParser[] timestampParsers)
    {
        ImmutableMap.Builder<Column, DynamicColumnSetter> builder = ImmutableMap.builder();
        for (ColumnConfig c : schema.getColumns()) {
            // TODO
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
            setter.set(unpacker.unpackValue().toString());
            // TODO set json?
            break;

        case EXTENSION:
            unpacker.skipValue();
            setter.setNull();
            // TODO set null
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
            } catch (EOFException ex) {
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
            } catch (EOFException ex) {
                return false;
            }
            for (int i = 0; i < n; i++) {
                MessageFormat format = unpacker.getNextFormat();
                if (!format.getValueType().isRawType()) {
                    unpacker.skipValue();
                    continue;
                }
                MessageBuffer key = unpacker.readPayloadAsReference(unpacker.unpackRawStringHeader());
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
