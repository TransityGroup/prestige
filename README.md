[![Master](https://travis-ci.org/smartcitiesdata/prestige.svg?branch=master)](https://travis-ci.org/smartcitiesdata/prestige)
[![Hex.pm Version](http://img.shields.io/hexpm/v/prestige.svg?style=flat)](https://hex.pm/packages/prestige)

# Prestige

A middleware layer for the Presto database

Documentation: [hexdocs](https://smartcolumbus_os.hexdocs.pm/prestige/)

## Installation

Prestige can be installed by adding `prestige` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:prestige, "~> 0.3.1"}
  ]
end
```

## Running tests
Clone the repo and fetch its dependencies:

```
$ git clone https://github.com/smartcolumbusos/prestige
$ cd prestige
$ mix deps.get
$ mix test
```

## Example Usage

### Adding Data to Presto

Create table:

```
create_statement = "create table memory.default.monkeys ( id bigint, stuff array(row(name varchar, color varchar)))"
Prestige.execute(create_statement, user: "prestouser") |> Stream.run()
```

Insert data:

```
insert_statement = "insert into memory.default.monkeys(id, stuff) values(1, array[row('George','black'), row('James','red')])"
Prestige.execute(insert_statement, user: "prestouser") |> Stream.run()
```

Select data:
```
iex> Prestige.execute("select * from memory.default.monkeys", user: "prestouser", by_names: true) |> Prestige.prefetch
[
  %{
    "id" => 1,
    "stuff" => [
      %{"color" => "black", "name" => "George"},
      %{"color" => "red", "name" => "James"}
    ]
  }
]
```

## License
Released under [Apache 2 license](https://github.com/SmartColumbusOS/prestige/blob/master/LICENSE).
