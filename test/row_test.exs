defmodule Prestige.RowTests do
  use ExUnit.Case

  setup do
    [bypass: Bypass.open(port: 8123)]
  end

  test "support row type in presto", %{bypass: bypass} do
    Bypass.expect(bypass, "POST", "/v1/statement", fn conn ->
      presto_response(conn)
    end)

    actual =
      Prestige.execute("select * from something", rows_as_maps: true)
      |> Prestige.prefetch()

    assert [%{"id" => 1, "stuff" => %{"name" => "Brian", "age" => 41}}] == actual
  end

  defp presto_response(conn, opts \\ []) do
    body = %{
      "state" => %{"state" => Keyword.get(opts, :state, "FINISHED")},
      "columns" => [
        %{
          "name" => "id",
          "type" => "bigint",
          "typeSignature" => %{
            "arguments" => [],
            "literalArguments" => [],
            "rawType" => "bigint",
            "typeArguments" => []
          }
        },
        %{
          "name" => "stuff",
          "type" => "row(name varchar,age integer)",
          "typeSignature" => %{
            "arguments" => [
              %{
                "kind" => "NAMED_TYPE_SIGNATURE",
                "value" => %{
                  "fieldName" => %{
                    "delimited" => false,
                    "name" => "name"
                  },
                  "typeSignature" => "varchar"
                }
              },
              %{
                "kind" => "NAMED_TYPE_SIGNATURE",
                "value" => %{
                  "fieldName" => %{
                    "delimited" => false,
                    "name" => "age"
                  },
                  "typeSignature" => "integer"
                }
              }
            ],
            "literalArguments" => [
              "name",
              "age"
            ],
            "rawType" => "row",
            "typeArguments" => [
              %{
                "arguments" => [
                  %{
                    "kind" => "LONG_LITERAL",
                    "value" => 2_147_483_647
                  }
                ],
                "literalArguments" => [],
                "rawType" => "varchar",
                "typeArguments" => []
              },
              %{
                "arguments" => [],
                "literalArguments" => [],
                "rawType" => "integer",
                "typeArguments" => []
              }
            ]
          }
        }
      ],
      "data" => [
        [
          1,
          [
            "Brian",
            41
          ]
        ]
      ]
    }

    Plug.Conn.put_resp_header(conn, "content-type", "application/json")
    |> Plug.Conn.resp(200, Jason.encode!(body))
  end
end
