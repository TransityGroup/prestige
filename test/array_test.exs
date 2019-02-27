defmodule Prestige.ArrayTest do
  use ExUnit.Case

  setup do
    [bypass: Bypass.open(port: 8123)]
  end

  test "nested row arrays become nested maps", %{bypass: bypass} do
    Bypass.expect(bypass, "POST", "/v1/statement", fn conn ->
      presto_response(conn)
    end)

    actual =
      Prestige.execute("select * from something", rows_as_maps: true)
      |> Prestige.prefetch()

    expected = [
      %{
        "id" => 2,
        "stuff" => [
          %{"name" => "Franky", "color" => "black"},
          %{"name" => "Big Bob", "color" => "yellow"}
        ]
      }
    ]

    assert expected == actual
  end

  defp presto_response(conn) do
    body = %{
      "addedPreparedStatements" => %{},
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
          "type" => "array(row(name varchar,color varchar))",
          "typeSignature" => %{
            "arguments" => [
              %{
                "kind" => "TYPE_SIGNATURE",
                "value" => %{
                  "arguments" => [
                    %{
                      "kind" => "NAMED_TYPE_SIGNATURE",
                      "value" => %{
                        "fieldName" => %{"delimited" => false, "name" => "name"},
                        "typeSignature" => "varchar"
                      }
                    },
                    %{
                      "kind" => "NAMED_TYPE_SIGNATURE",
                      "value" => %{
                        "fieldName" => %{"delimited" => false, "name" => "color"},
                        "typeSignature" => "varchar"
                      }
                    }
                  ],
                  "literalArguments" => ["name", "color"],
                  "rawType" => "row",
                  "typeArguments" => [
                    %{
                      "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
                      "literalArguments" => [],
                      "rawType" => "varchar",
                      "typeArguments" => []
                    },
                    %{
                      "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
                      "literalArguments" => [],
                      "rawType" => "varchar",
                      "typeArguments" => []
                    }
                  ]
                }
              }
            ],
            "literalArguments" => [],
            "rawType" => "array",
            "typeArguments" => [
              %{
                "arguments" => [
                  %{
                    "kind" => "NAMED_TYPE_SIGNATURE",
                    "value" => %{
                      "fieldName" => %{"delimited" => false, "name" => "name"},
                      "typeSignature" => "varchar"
                    }
                  },
                  %{
                    "kind" => "NAMED_TYPE_SIGNATURE",
                    "value" => %{
                      "fieldName" => %{"delimited" => false, "name" => "color"},
                      "typeSignature" => "varchar"
                    }
                  }
                ],
                "literalArguments" => ["name", "color"],
                "rawType" => "row",
                "typeArguments" => [
                  %{
                    "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
                    "literalArguments" => [],
                    "rawType" => "varchar",
                    "typeArguments" => []
                  },
                  %{
                    "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
                    "literalArguments" => [],
                    "rawType" => "varchar",
                    "typeArguments" => []
                  }
                ]
              }
            ]
          }
        }
      ],
      "data" => [[2, [["Franky", "black"], ["Big Bob", "yellow"]]]],
      "deallocatedPreparedStatements" => [],
      "id" => "20190223_000127_00004_tf6fe",
      "infoUri" => "http://localhost:8080/ui/query.html?20190223_000127_00004_tf6fe",
      "partialCancelUri" => "http://172.17.0.2:8080/v1/stage/20190223_000127_00004_tf6fe.0",
      "stats" => %{
        "completedSplits" => 8,
        "cpuTimeMillis" => 4,
        "elapsedTimeMillis" => 111,
        "nodes" => 1,
        "peakMemoryBytes" => 0,
        "processedBytes" => 131,
        "processedRows" => 2,
        "progressPercentage" => 33.333333333333336,
        "queued" => false,
        "queuedSplits" => 14,
        "queuedTimeMillis" => 2,
        "rootStage" => %{
          "completedSplits" => 0,
          "cpuTimeMillis" => 0,
          "done" => false,
          "nodes" => 1,
          "processedBytes" => 0,
          "processedRows" => 0,
          "queuedSplits" => 14,
          "runningSplits" => 2,
          "stageId" => "0",
          "state" => "RUNNING",
          "subStages" => [
            %{
              "completedSplits" => 8,
              "cpuTimeMillis" => 4,
              "done" => true,
              "nodes" => 1,
              "processedBytes" => 131,
              "processedRows" => 2,
              "queuedSplits" => 0,
              "runningSplits" => 0,
              "stageId" => "1",
              "state" => "FINISHED",
              "subStages" => [],
              "totalSplits" => 8,
              "wallTimeMillis" => 23
            }
          ],
          "totalSplits" => 16,
          "wallTimeMillis" => 4
        },
        "runningSplits" => 2,
        "scheduled" => true,
        "state" => "RUNNING",
        "totalSplits" => 24,
        "wallTimeMillis" => 27
      },
      "warnings" => []
    }

    conn
    |> Plug.Conn.put_resp_header("content-type", "application/json")
    |> Plug.Conn.resp(200, Jason.encode!(body))
  end
end
