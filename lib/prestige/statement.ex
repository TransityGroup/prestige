defmodule Prestige.Statement do
  use Tesla

  plug Tesla.Middleware.BaseUrl, Application.get_env(:prestige, :base_url)
  plug Tesla.Middleware.Headers, [{"content-type", "text/plain"}]
  plug Prestige.Middleware.Retry, delay: 100, max_retries: 5
  plug Tesla.Middleware.Logger, log_level: :debug
  plug Tesla.Middleware.DecodeJson

  def execute(statement, opts \\ []) do
    Stream.resource(
      fn -> initial_accumulator(statement, opts) end,
      &next/1,
      fn _ -> :ok end
    )
  end

  defp initial_accumulator(statement, opts) do
    {by_names, header_opts} = Keyword.get_and_update(opts, :by_names, fn _ -> :pop end)
    headers = create_headers(header_opts) |> IO.inspect

    %{response: post("/v1/statement", statement, headers: headers), by_names: by_names || false}
  end

  defp next(%{response: response, by_names: by_names}) do
    transform(response, by_names)
  end

  defp next(%{next_uri: nil}), do: {:halt, :ok}

  defp next(%{next_uri: next_uri, by_names: by_names}) do
    next_uri
    |> get()
    |> transform(by_names)
  end

  defp transform({:ok, %Tesla.Env{status: 200, body: body}}, by_names) do
    data =
      Map.get(body, "data", [])
      |> Enum.map(&transform_row(&1, Map.get(body, "columns", []), by_names))

    {data, %{next_uri: body["nextUri"], by_names: by_names}}
  end

  defp transform_row(row, _columns, false), do: row

  defp transform_row(row, columns, true) do
    columns
    |> Enum.map(fn col -> col["name"] end)
    |> Enum.zip(row)
    |> Enum.into(%{})
  end

  defp create_headers(opts) do
    Application.get_env(:prestige, :headers)
    |> Keyword.merge(opts)
    |> Enum.map(&create_header/1)
  end

  defp create_header({name, value}) when is_atom(name) do
    presto_name =
      name
      |> to_string()
      |> String.split("_")
      |> Enum.map_join("-", &String.capitalize(&1))

    {"X-Presto-#{presto_name}", value}
  end

  def prefetch(result) do
    Enum.map(result, fn x -> x end)
  end
end
