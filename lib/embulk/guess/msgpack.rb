module Embulk
  module Guess

    class Msgpack < GuessPlugin
      Plugin.register_guess("msgpack", self)

      def guess(config, sample_buffer)
        return {} unless config.fetch("parser", {}).fetch("type", "msgpack") == "msgpack"

        parser_config = config["parser"] || {}

        classpath = File.expand_path('../../../../classpath', __FILE__)
        Dir["#{classpath}/*.jar"].each {|jar| require jar }

        file_encoding = parser_config["file_encoding"]
        row_encoding = parser_config["row_encoding"]

        if !file_encoding || !row_encoding
          uk = new_unpacker(sample_buffer)
          begin
            n = uk.unpackArrayHeader
            begin
              n = uk.unpackArrayHeader
              file_encoding = "array"
              row_encoding = "array"
            rescue org.msgpack.core.MessageTypeException
              file_encoding = "sequence"
              row_encoding = "array"
            end
          rescue org.msgpack.core.MessageTypeException
            uk = new_unpacker(sample_buffer)  # TODO unpackArrayHeader consumes buffer (unexpectedly)
            begin
              n = uk.unpackMapHeader
              file_encoding = "sequence"
              row_encoding = "map"
            rescue org.msgpack.core.MessageTypeException
              return {}  # not a msgpack
            end
          end
        end

        uk = new_unpacker(sample_buffer)

        case file_encoding
        when "array"
          uk.unpackArrayHeader  # skip array header to convert to sequence
        when "sequence"
          # do nothing
        end

        rows = []

        begin
          while true
            rows << JSON.parse(uk.unpackValue.toJson)
          end
        rescue java.io.EOFException
        end

        if rows.size <= 3
          return {}
        end

        case row_encoding
        when "map"
          schema = Embulk::Guess::SchemaGuess.from_hash_records(rows)
        when "array"
          column_count = rows.map {|r| r.size }.max
          column_names = column_count.times.map {|i| "c#{i}" }
          schema = Embulk::Guess::SchemaGuess.from_array_records(column_names, rows)
        end

        parser_guessed = {"type" => "msgpack"}
        parser_guessed["row_encoding"] = row_encoding
        parser_guessed["file_encoding"] = file_encoding
        parser_guessed["columns"] = schema

        return {"parser" => parser_guessed}

      rescue org.msgpack.core.MessagePackException
        return {}
      end

      def new_unpacker(sample_buffer)
        org.msgpack.core.MessagePack.newDefaultUnpacker(sample_buffer.to_java_bytes)
      end
    end
  end
end
