[![Master](https://travis-ci.org/smartcitiesdata/prestige.svg?branch=master)](https://travis-ci.org/smartcitiesdata/prestige)
[![Hex.pm Version](http://img.shields.io/hexpm/v/prestige.svg?style=flat)](https://hex.pm/packages/prestige)

# Prestige

A middleware layer for the Presto database

Documentation: [hexdocs](https://hexdocs.pm/prestige)

## Installation

Prestige can be installed by adding `prestige` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:prestige, "~> 1.0.0"}
  ]
end
```

## Running tests
Clone the repo and fetch its dependencies:

```
$ git clone https://github.com/smartcitiesdata/prestige
$ cd prestige
$ mix deps.get
$ mix test
```

## Example Usage

### Adding Data to Presto

Create table:

```
session = Prestige.new_session(url: "http://localhost:8080", user: "bbalser")
create_statement = "create table memory.default.monkeys ( id bigint, stuff array(row(name varchar, color varchar)))"
Prestige.query(session, create_statement)
```

Insert data:

```
session = Prestige.new_session(url: "http://localhost:8080", user: "bbalser")
insert_statement = "insert into memory.default.monkeys(id, stuff) values(1, array[row('George','black'), row('James','red')])"
Prestige.query(session, insert_statement)
```

 or 

```
session = Prestige.new_session(url: "http://localhost:8080", user: "bbalser")
insert_statement = "insert into memory.default.monkeys(id, stuff) values(?, array[row(?,?), row(?,?)])"
Prestige.query(session, insert_statement, [1, "George", "black", "James", "red"])
```

Select data:
```

iex> session = Prestige.new_session(url: "http://localhost:8080", user: "bbalser")
iex> Prestige.query!(session, "select * from memory.default.monkeys") |> Prestige.Result.as_maps()
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
Released under [Apache 2 license](https://github.com/smartcitiesdata/prestige/blob/master/LICENSE).
