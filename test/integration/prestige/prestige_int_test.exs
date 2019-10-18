defmodule Prestige.IntTest do
  use ExUnit.Case
  use Divo

  alias Prestige.PrestoClient

  @moduletag capture_log: true

  @session Prestige.new_session(url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default")

  setup_all do
    exec("CREATE TABLE IF NOT EXISTS people(name varchar, age int)")
    exec("CREATE TABLE IF NOT EXISTS people2(name varchar, age int)")

    PrestoClient.execute(
      @session,
      "stmt",
      "INSERT INTO people(name, age) VALUES('george', 10), ('pete', 20)",
      []
    )
    |> Enum.to_list()

    :ok
  end

  test "simple query" do
    {:ok, result} = Prestige.query(@session, "select * from people")

    assert result.rows == [["george", 10], ["pete", 20]]
    assert [%{"name" => "george", "age" => 10}, %{"name" => "pete", "age" => 20}] == Prestige.Result.as_maps(result)
  end

  test "query with arguments" do
    {:ok, result} = Prestige.query(@session, "select * from people where name = ?", ["george"])

    assert result.rows == [["george", 10]]
  end

  test "stream results" do
    results =
      Prestige.stream!(@session, "select * from people")
      |> Enum.map(&Prestige.Result.as_maps/1)
      |> List.flatten()

    assert [%{"name" => "george", "age" => 10}, %{"name" => "pete", "age" => 20}] == results
  end

  test "rollback transaction" do
    Prestige.transaction(@session, fn session ->
      Prestige.query(session, "insert into people(name, age) values('joe', 19)")

      assert [%{"name" => "joe", "age" => 19}] ==
               Prestige.query!(session, "select * from people where name = 'joe'")
               |> Prestige.Result.as_maps()

      :rollback
    end)

    assert [] ==
             Prestige.query!(@session, "select * from people where name = 'joe'")
             |> Prestige.Result.as_maps()
  end

  test "commit with return value" do
    result =
      Prestige.transaction(@session, fn session ->
        Prestige.query(session, "insert into people2(name, age) values('joe', 19)")

        maps =
          Prestige.query!(session, "select * from people2 where name = 'joe'")
          |> Prestige.Result.as_maps()

        {:commit, maps}
      end)

    assert [%{"name" => "joe", "age" => 19}] == result

    assert result ==
             Prestige.query!(@session, "select * from people2 where name = 'joe'")
             |> Prestige.Result.as_maps()
  end

  test "transaction should rollback when error is raised" do
    Prestige.transaction(@session, fn s ->
      Prestige.query(s, "insert into people(name, age) values('harry', 10)")
      raise "something went wrong"
      :commit
    end)

    flunk("Should have reraised the exception")
  rescue
    e ->
      assert e == RuntimeError.exception(message: "something went wrong")
      assert [] == Prestige.query!(@session, "select * from  people where name = 'harry'") |> Prestige.Result.as_maps()
  end

  defp exec(statement) do
    PrestoClient.execute(@session, "stmt", statement, []) |> Enum.to_list()
  end
end
