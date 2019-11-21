defmodule Prestige.IntTest do
  use ExUnit.Case
  use Divo

  alias Prestige.Client

  @moduletag capture_log: true

  @session Prestige.new_session(
             url: "http://localhost:8080",
             user: "bbalser",
             catalog: "hive",
             schema: "default",
             receive_timeout: 10_000
           )

  setup_all do
    exec("CREATE TABLE IF NOT EXISTS tx_people(name varchar, age int)")
    exec("CREATE TABLE IF NOT EXISTS tx_people2(name varchar, age int)")

    Client.execute(
      @session,
      "stmt",
      "INSERT INTO tx_people(name, age) VALUES('george', 10), ('pete', 20)",
      []
    )
    |> Enum.to_list()

    :ok
  end

  test "rollback transaction" do
    Prestige.transaction(@session, fn session ->
      Prestige.query(session, "insert into tx_people(name, age) values('joe', 19)")

      assert [%{"name" => "joe", "age" => 19}] ==
               Prestige.query!(session, "select * from tx_people where name = 'joe'")
               |> Prestige.Result.as_maps()

      :rollback
    end)

    assert [] ==
             Prestige.query!(@session, "select * from tx_people where name = 'joe'")
             |> Prestige.Result.as_maps()
  end

  test "commit with return value" do
    result =
      Prestige.transaction(@session, fn session ->
        Prestige.query(session, "insert into tx_people2(name, age) values('joe', 19)")

        maps =
          Prestige.query!(session, "select * from tx_people2 where name = 'joe'")
          |> Prestige.Result.as_maps()

        {:commit, maps}
      end)

    assert [%{"name" => "joe", "age" => 19}] == result

    assert result ==
             Prestige.query!(@session, "select * from tx_people2 where name = 'joe'")
             |> Prestige.Result.as_maps()
  end

  test "transaction should rollback when error is raised" do
    Prestige.transaction(@session, fn s ->
      Prestige.query(s, "insert into tx_people(name, age) values('harry', 10)")
      raise "something went wrong"
      :commit
    end)

    flunk("Should have reraised the exception")
  rescue
    e ->
      assert e == RuntimeError.exception(message: "something went wrong")

      assert [] ==
               Prestige.query!(@session, "select * from  tx_people where name = 'harry'") |> Prestige.Result.as_maps()
  end

  defp exec(statement) do
    Client.execute(@session, "stmt", statement, []) |> Enum.to_list()
  end
end
