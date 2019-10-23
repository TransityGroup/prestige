defmodule PrestigeTest do
  use Prestige.Case
  use Divo

  @moduletag capture_log: true

  session url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default"

  table "people", %{
    "name" => "varchar",
    "age" => "int"
  }

  table "cakes", %{
    "id" => "int",
    "name" => "varchar"
  }

  table "friends", %{
    "name" => "varchar",
    "friends" => "array(row(name varchar, age int))",
    "colors" => "array(varchar)",
    "spouse" => "row(name varchar, age int)"
  }

  table "colors", %{
    "name" => "varchar",
    "colors" => "array(varchar)"
  }

  test "simple query", %{session: session} do
    Prestige.query!(session, "insert into people(name, age) values('george', 10), ('pete', 20)")
    {:ok, result} = Prestige.query(session, "select * from people")

    assert result.rows == [[10, "george"], [20, "pete"]]
    assert [%{"name" => "george", "age" => 10}, %{"name" => "pete", "age" => 20}] == Prestige.Result.as_maps(result)
  end

  test "query with arguments", %{session: session} do
    Prestige.query!(session, "insert into people(name, age) values('george', 10), ('pete', 20)")
    {:ok, result} = Prestige.query(session, "select * from people where name = ?", ["george"])

    assert result.rows == [[10, "george"]]
  end

  test "stream results", %{session: session} do
    Prestige.query!(session, "insert into people(name, age) values('george', 10), ('pete', 20)")

    results =
      Prestige.stream!(session, "select * from people order by age")
      |> Enum.map(&Prestige.Result.as_maps/1)
      |> List.flatten()

    assert [%{"name" => "george", "age" => 10}, %{"name" => "pete", "age" => 20}] == results
  end

  test "as_maps supports arrays", %{session: session} do
    Prestige.query!(session, "insert into colors(name, colors) values('joe', array['blue', 'green'])")

    {:ok, result} = Prestige.query(session, "select * from colors")

    expected = %{
      "name" => "joe",
      "colors" => ["blue", "green"]
    }

    assert [expected] == Prestige.Result.as_maps(result)
  end

  test "as_maps support hierarchical data", %{session: session} do
    args = ["pete", "chris", 21, "bob", 72, "blue", "green", "shirley", 21]

    Prestige.query!(
      session,
      "insert into friends(name, friends, colors, spouse) values(?, array[row(?, ?), row(?, ?)], array[?, ?], row(?, ?))",
      args
    )

    {:ok, result} = Prestige.query(session, "select * from friends")

    expected = %{
      "name" => "pete",
      "friends" => [
        %{"name" => "chris", "age" => 21},
        %{"name" => "bob", "age" => 72}
      ],
      "colors" => ["blue", "green"],
      "spouse" => %{"name" => "shirley", "age" => 21}
    }

    assert [expected] == Prestige.Result.as_maps(result)
  end

  test "can prepared and use a prepared statement", %{session: session} do
    {:ok, prepared_session} = Prestige.prepare(session, "insert_cakes", "insert into cakes(id, name) values(?, ?)")
    {:ok, _result} = Prestige.execute(prepared_session, "insert_cakes", [1, "Red Velvet"])

    on_exit(fn ->
      {:ok, _session} = Prestige.close(prepared_session, "insert_cakes")
    end)

    assert [%{"id" => 1, "name" => "Red Velvet"}] ==
             Prestige.query!(session, "select * from cakes") |> Prestige.Result.as_maps()
  end
end
