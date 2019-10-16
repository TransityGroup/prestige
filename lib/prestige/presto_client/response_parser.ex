defmodule Prestige.PrestoClient.ResponseParser do
  @moduledoc false

  defmodule Accumulator do
    @moduledoc false
    @presto_prefix "x-presto"

    defstruct headers: [],
              sent: false,
              columns: []

    def new(opts \\ []) do
      struct!(__MODULE__, opts)
    end

    def new(%__MODULE__{} = last, opts) do
      headers = merge_presto_headers(last, Keyword.get(opts, :headers, []))

      Map.merge(last, Map.new(opts))
      |> Map.put(:headers, headers)
    end

    defp merge_presto_headers(%__MODULE__{} = last, headers) do
      last.headers ++
        Enum.filter(headers, fn {name, _value} -> String.starts_with?(name, @presto_prefix) end)
    end
  end

  def parse(request_stream) do
    Stream.chunk_while(
      request_stream,
      Accumulator.new(),
      &chunk/2,
      &post/1
    )
  end

  defp chunk(%{body: %{"error" => error}}, _acc) do
    raise Prestige.Error,
      message: error["message"],
      code: error["errorCode"],
      location: error["errorLocation"],
      name: error["errorName"],
      type: error["errorType"],
      stack: get_in(error, ["failureInfo", "stack"])
  end

  defp chunk(%{body: body, headers: headers}, acc) do
    columns = Map.get(body, "columns", []) |> transform_columns()
    next_acc = Accumulator.new(acc, headers: headers, columns: columns)

    case Map.get(body, "data", []) do
      [] ->
        {:cont, next_acc}

      data ->
        result = %Prestige.Result{
          columns: columns,
          rows: data,
          presto_headers: next_acc.headers
        }

        {:cont, result, %{next_acc | sent: true}}
    end
  end

  defp post(acc) do
    case acc.sent do
      false ->
        result = %Prestige.Result{
          columns: acc.columns,
          rows: [],
          presto_headers: acc.headers
        }

        {:cont, result, acc}

      true ->
        {:cont, acc}
    end
  end

  defp transform_columns(columns) do
    Enum.map(columns, fn %{"name" => name, "type" => type} ->
      %Prestige.ColumnDefinition{name: name, type: type}
    end)
  end
end
