# Prestige

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `prestige` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:prestige, "~> 0.3.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/prestige](https://hexdocs.pm/prestige).


## Testing

### Adding Data to Presto

`create table memory.default.monkeys ( id bigint, stuff array(row(name varchar, color varchar)));`

`insert into memory.default.monkeys(id, stuff) values(1, array[row('George','black'), row('James','red')]);`


`create table memory.default.fruit ( id bigint, stuff array(bigint));`

`insert into memory.default.fruit(id, stuff) values(1, array[1,2,3]);`

### Getting data back through IEX

`Prestige.execute("select * from memory.default.fruit", user: "bbalser", by_names: true) |> Prestige.prefetch`
