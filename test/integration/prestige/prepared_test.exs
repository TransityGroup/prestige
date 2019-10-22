defmodule PreparedTest do
  use Prestige.Case
  use Divo

  @moduletag capture_log: true

  session url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default"
  table "cakes", %{
    "id" => "int",
    "name" => "varchar"
  }

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
