defmodule Prestige.CaseTest do
  use Prestige.Case
  use Divo

  session url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default"
  table "people(name varchar, age int)"

  test "brian", %{session: session} do
    Prestige.query!(session, "insert into people(name, age) values('Brian', 21)")

    assert [%{"name" => "Brian"}] == Prestige.query!(session, "select name from people") |> Prestige.Result.as_maps()
  end

  test "johnson", %{session: session} do
    Prestige.query!(session, "insert into people(name, age) values('joe', 19)")

    assert [%{"name" => "joe"}] == Prestige.query!(session, "select name from people") |> Prestige.Result.as_maps()
  end
end
