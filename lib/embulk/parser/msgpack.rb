Embulk::JavaPlugin.register_parser(
  "msgpack", "org.embulk.parser.MsgpackParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
