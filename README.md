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

```
$ embulk gem install embulk-parser-msgpack
$ embulk guess -g msgpack config.yml -o guessed.yml
```

## Build

```
$ ./gradlew gem
```
