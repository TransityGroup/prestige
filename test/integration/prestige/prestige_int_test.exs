defmodule Prestige.IntTest do
  use ExUnit.Case
  use Divo

  alias Prestige.PrestoClient

  @moduletag capture_log: true

  @session Prestige.new_session(host: "http://localhost:8080", user: "bbalser")

  setup_all do
    PrestoClient.execute(@session, "stmt", "CREATE TABLE memory.default.people(name varchar, age int)", [])
    |> Enum.to_list()

    PrestoClient.execute(
      @session,
      "stmt",
      "INSERT INTO memory.default.people(name, age) VALUES('george', 10), ('pete', 20)",
      []
    )
    |> Enum.to_list()

    :ok
  end

  test "simple query" do
    {:ok, result} = Prestige.query(@session, "select * from memory.default.people")

    assert result.rows == [["george", 10], ["pete", 20]]
    assert [%{"name" => "george", "age" => 10}, %{"name" => "pete", "age" => 20}] == Prestige.Result.as_maps(result)
  end

  test "query with arguments" do
    {:ok, result} = Prestige.query(@session, "select * from memory.default.people where name = ?", ["george"])

    assert result.rows == [["george", 10]]
  end

  test "stream results" do
    result = Prestige.stream!(@session, "select * from memory.default.people") |> Enum.to_list()

    assert [%{rows: [["george", 10], ["pete", 20]]}] = result
  end

  test "transaction" do
    Prestige.transaction(@session, fn session ->
      Prestige.query(session, "insert into memory.default.people(name, age) values('joe', 19)")

      assert [%{"name" => "joe", "age" => 19}] ==
               Prestige.query(session, "select * from memory.default.people where name = 'joe'")
               |> Prestige.Result.as_maps()
    end)
  end
end
