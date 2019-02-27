defmodule Prestige.SimpleArrayTest do
  use ExUnit.Case

  setup do
    [bypass: Bypass.open(port: 8123)]
  end

  test "nested simple arrays remain lists", %{bypass: bypass} do
    Bypass.expect(bypass, "POST", "/v1/statement", fn conn ->
      presto_response(conn)
    end)

    actual =
      Prestige.execute("select * from something", rows_as_maps: true)
      |> Prestige.prefetch()

    expected = [
      %{
        "id" => 1,
        "stuff" => [
          1,
          2,
          3
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
          "type" => "array(bigint)",
          "typeSignature" => %{
            "arguments" => [
              %{
                "kind" => "TYPE_SIGNATURE",
                "value" => %{
                  "arguments" => [],
                  "literalArguments" => [],
                  "rawType" => "bigint",
                  "typeArguments" => []
                }
              }
            ],
            "literalArguments" => [],
            "rawType" => "array",
            "typeArguments" => [
              %{
                "arguments" => [],
                "literalArguments" => [],
                "rawType" => "bigint",
                "typeArguments" => []
              }
            ]
          }
        }
      ],
      "data" => [[1, [1, 2, 3]]],
      "deallocatedPreparedStatements" => [],
      "id" => "20190223_040752_00018_tf6fe",
      "infoUri" => "http://localhost:8080/ui/query.html?20190223_040752_00018_tf6fe",
      "partialCancelUri" => "http://172.17.0.2:8080/v1/stage/20190223_040752_00018_tf6fe.0",
      "stats" => %{
        "completedSplits" => 1,
        "cpuTimeMillis" => 6,
        "elapsedTimeMillis" => 114,
        "nodes" => 1,
        "peakMemoryBytes" => 0,
        "processedBytes" => 41,
        "processedRows" => 1,
        "progressPercentage" => 4.166666666666667,
        "queued" => false,
        "queuedSplits" => 8,
        "queuedTimeMillis" => 2,
        "rootStage" => %{
          "completedSplits" => 0,
          "cpuTimeMillis" => 1,
          "done" => false,
          "nodes" => 1,
          "processedBytes" => 0,
          "processedRows" => 0,
          "queuedSplits" => 8,
          "runningSplits" => 8,
          "stageId" => "0",
          "state" => "RUNNING",
          "subStages" => [
            %{
              "completedSplits" => 1,
              "cpuTimeMillis" => 5,
              "done" => true,
              "nodes" => 1,
              "processedBytes" => 41,
              "processedRows" => 1,
              "queuedSplits" => 0,
              "runningSplits" => 7,
              "stageId" => "1",
              "state" => "FINISHED",
              "subStages" => [],
              "totalSplits" => 8,
              "wallTimeMillis" => 199
            }
          ],
          "totalSplits" => 16,
          "wallTimeMillis" => 40
        },
        "runningSplits" => 15,
        "scheduled" => true,
        "state" => "RUNNING",
        "totalSplits" => 24,
        "wallTimeMillis" => 239
      },
      "warnings" => []
    }

    conn
    |> Plug.Conn.put_resp_header("content-type", "application/json")
    |> Plug.Conn.resp(200, Jason.encode!(body))
  end
end
