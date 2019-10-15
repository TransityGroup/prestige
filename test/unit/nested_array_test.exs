defmodule Prestige.NestedArrayTest do
  use ExUnit.Case

  setup do
    [bypass: Bypass.open(port: 8123)]
  end

  test "nested rows with arrays of rows become nested maps", %{bypass: bypass} do
    data = [[1, ["George", [["Bob", 7], ["Joe", 14], ["Lou", 2]]]]]

    Bypass.expect(bypass, "POST", "/v1/statement", fn conn ->
      presto_response(conn, data)
    end)

    actual =
      Prestige.execute("select * from something", rows_as_maps: true)
      |> Prestige.prefetch()

    expected = [
      %{
        "id" => 1,
        "stuff" => %{
          "name" => "George",
          "friends" => [
            %{"name" => "Bob", "age" => 7},
            %{"name" => "Joe", "age" => 14},
            %{"name" => "Lou", "age" => 2}
          ]
        }
      }
    ]

    assert expected == actual
  end

  test "top level column with with nils", %{bypass: bypass} do
    data = [[1, nil]]

    Bypass.expect(bypass, "POST", "/v1/statement", fn conn ->
      presto_response(conn, data)
    end)

    actual =
      Prestige.execute("select * from something", rows_as_maps: true)
      |> Prestige.prefetch()

    expected = [
      %{
        "id" => 1,
        "stuff" => nil
      }
    ]

    assert expected == actual
  end

  defp presto_response(conn, data) do
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
          "type" => "row(name varchar,friends array(row(name varchar,age integer)))",
          "typeSignature" => %{
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
                  "fieldName" => %{"delimited" => false, "name" => "friends"},
                  "typeSignature" => "array(row(name varchar,age integer))"
                }
              }
            ],
            "literalArguments" => ["name", "friends"],
            "rawType" => "row",
            "typeArguments" => [
              %{
                "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
                "literalArguments" => [],
                "rawType" => "varchar",
                "typeArguments" => []
              },
              %{
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
                            "fieldName" => %{"delimited" => false, "name" => "age"},
                            "typeSignature" => "integer"
                          }
                        }
                      ],
                      "literalArguments" => ["name", "age"],
                      "rawType" => "row",
                      "typeArguments" => [
                        %{
                          "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
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
                          "fieldName" => %{"delimited" => false, "name" => "age"},
                          "typeSignature" => "integer"
                        }
                      }
                    ],
                    "literalArguments" => ["name", "age"],
                    "rawType" => "row",
                    "typeArguments" => [
                      %{
                        "arguments" => [%{"kind" => "LONG_LITERAL", "value" => 2_147_483_647}],
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
                ]
              }
            ]
          }
        }
      ],
      "data" => data,
      "deallocatedPreparedStatements" => [],
      "id" => "20190223_064615_00031_tf6fe",
      "infoUri" => "http://localhost:8080/ui/query.html?20190223_064615_00031_tf6fe",
      "partialCancelUri" => "http://172.17.0.2:8080/v1/stage/20190223_064615_00031_tf6fe.1",
      "stats" => %{
        "completedSplits" => 8,
        "cpuTimeMillis" => 3,
        "elapsedTimeMillis" => 83,
        "nodes" => 1,
        "peakMemoryBytes" => 0,
        "processedBytes" => 84,
        "processedRows" => 1,
        "progressPercentage" => 33.333333333333336,
        "queued" => false,
        "queuedSplits" => 16,
        "queuedTimeMillis" => 1,
        "rootStage" => %{
          "completedSplits" => 0,
          "cpuTimeMillis" => 0,
          "done" => false,
          "nodes" => 1,
          "processedBytes" => 0,
          "processedRows" => 0,
          "queuedSplits" => 16,
          "runningSplits" => 0,
          "stageId" => "0",
          "state" => "RUNNING",
          "subStages" => [
            %{
              "completedSplits" => 8,
              "cpuTimeMillis" => 3,
              "done" => false,
              "nodes" => 1,
              "processedBytes" => 84,
              "processedRows" => 1,
              "queuedSplits" => 0,
              "runningSplits" => 0,
              "stageId" => "1",
              "state" => "RUNNING",
              "subStages" => [],
              "totalSplits" => 8,
              "wallTimeMillis" => 26
            }
          ],
          "totalSplits" => 16,
          "wallTimeMillis" => 0
        },
        "runningSplits" => 0,
        "scheduled" => true,
        "state" => "RUNNING",
        "totalSplits" => 24,
        "wallTimeMillis" => 26
      },
      "warnings" => []
    }

    conn
    |> Plug.Conn.put_resp_header("content-type", "application/json")
    |> Plug.Conn.resp(200, Jason.encode!(body))
  end
end
