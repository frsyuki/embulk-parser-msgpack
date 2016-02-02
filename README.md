# MessagePack parser plugin for Embulk

Parses files encoded in MessagePack.

## Overview

* **Plugin type**: parser
* **Guess supported**: yes

## Configuration

- **row_encoding**: type of a row. "array" or "map" (enum, default: map)
- **file_encoding**: if a file includes a big array, set "array". Otherwise, if a file includes sequence of rows, set "sequence" (enum, default: sequence)
- **columns**: description (schema, required)

## Example

seed.yml:

```yaml
in:
  # here can use any file input plugin type such as file, s3, gcs, etc.
  type: file
  path_prefix: /path/to/file/or/directory
  parser:
    type: msgpack
```

Command:

```
$ embulk gem install embulk-parser-msgpack
$ embulk guess -g msgpack seed.yml -o config.yml
$ embulk run config.yml
```

The guessed config.yml will include column settings:

```yaml
in:
  type: any file input plugin type
  parser:
    type: msgpack
    row_encoding: map
    file_encoding: sequence
    columns:
    - {index: 0, name: a, type: long}
    - {index: 1, name: b, type: string}
```


## Build

```
$ ./gradlew gem
```
