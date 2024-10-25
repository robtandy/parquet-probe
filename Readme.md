## Building

```shell
cargo build
```

## Running

after running,

```shell
./target/debug/parquet-probe --help
```

or all in one step

```shell
cargo run -- --help
```

## Keyboard bindings

- `DOWN`: Decrement the Row Group for current file
- `UP`: Increment the Row Group for current file
- `RIGHT`: Decrement the Column number for current file
- `LEFT`: Decrement the Column number for current file
- `TAB`: Move current file focus to next file to the right
- `ESC`: Quit
