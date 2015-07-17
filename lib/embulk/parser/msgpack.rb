Embulk::JavaPlugin.register_parser(
  "msgpack", "org.embulk.parser.msgpack.MsgpackParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
