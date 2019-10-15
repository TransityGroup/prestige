defmodule Prestige.ErrorTest do
  require TemporaryEnv
  use ExUnit.Case

  setup do
    [bypass: Bypass.open(port: 8123)]
  end

  test "prestige should raise exception when a http status 400 is returned", %{bypass: bypass} do
    Bypass.stub(bypass, "POST", "/v1/statement", fn conn ->
      Plug.Conn.resp(conn, 400, "User must be set")
    end)

    assert_raise(Prestige.BadRequestError, "User must be set", fn ->
      Prestige.execute("select * from memory.default.table") |> Prestige.prefetch()
    end)
  end

  test "prestige should raise an error when an error is returned by presto", %{bypass: bypass} do
    Bypass.stub(bypass, "POST", "/v1/statement", fn conn ->
      body = %{
        "error" => %{
          "errorCode" => 27,
          "errorLocation" => %{"columnNumber" => 16, "lineNumber" => 1},
          "errorName" => "SYNTAX_ERROR",
          "errorType" => "USER_ERROR",
          "message" => "error message",
          "failureInfo" => %{
            "stack" => ["one", "two"]
          }
        }
      }

      conn
      |> Plug.Conn.put_resp_header("content-type", "application/json")
      |> Plug.Conn.resp(200, Jason.encode!(body))
    end)

    try do
      Prestige.execute("select * from table") |> Prestige.prefetch()
      flunk("A Prestige.Error should have been raised")
    rescue
      e in Prestige.Error ->
        assert e.message == "error message"
        assert e.code == 27
        assert e.location == %{"columnNumber" => 16, "lineNumber" => 1}
        assert e.name == "SYNTAX_ERROR"
        assert e.type == "USER_ERROR"
        assert e.stack == ["one", "two"]
    end
  end

  @tag capture_log: true
  test "prestige should raise an error when it cannot connect to presto (econnrefused)", %{bypass: _bypass} do
    TemporaryEnv.put :prestige, :base_url, "localhost:65432" do
      assert_raise(Prestige.ConnectionError, fn ->
        Prestige.execute("SELECT * FROM TABLES") |> Prestige.prefetch()
      end)
    end
  end
end
