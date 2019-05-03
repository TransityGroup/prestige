defmodule Prestige.Statement do
  @moduledoc """
  Handles the request response setup for presto
  """
  use Tesla
  alias Prestige.Result

  plug(Tesla.Middleware.BaseUrl, Application.get_env(:prestige, :base_url))
  plug(Tesla.Middleware.Headers, [{"content-type", "text/plain"}])
  plug(Prestige.Middleware.Retry, delay: 100, max_retries: 5)
  plug(Tesla.Middleware.Logger, log_level: :debug)
  plug(Tesla.Middleware.DecodeJson)

  def execute(statement, opts \\ []) do
    Stream.resource(
      fn -> initial_accumulator(statement, opts) end,
      &next/1,
      fn _ -> :ok end
    )
  end

  def prefetch(result) do
    Enum.to_list(result)
  end

  defp initial_accumulator(statement, opts) do
    headers = opts |> Keyword.drop([:rows_as_maps, :by_names]) |> create_headers()

    %{response: post("/v1/statement", statement, headers: headers), rows_as_maps: rows_as_maps?(opts)}
  end

  defp next(%{response: response, rows_as_maps: rows_as_maps}) do
    Result.transform(response, rows_as_maps)
  end

  defp next(%{next_uri: nil}), do: {:halt, :ok}

  defp next(%{next_uri: next_uri, rows_as_maps: rows_as_maps}) do
    next_uri
    |> get()
    |> Result.transform(rows_as_maps)
  end

  defp create_headers(opts) do
    Application.get_env(:prestige, :headers, [])
    |> Keyword.merge(opts)
    |> Enum.map(&create_header/1)
  end

  defp create_header({name, value}) when is_atom(name) do
    presto_name =
      name
      |> to_string()
      |> String.split("_")
      |> Enum.map_join("-", &String.capitalize/1)

    {"X-Presto-#{presto_name}", value}
  end

  defp rows_as_maps?(opts) do
    Keyword.get_lazy(opts, :rows_as_maps, fn ->
      Keyword.get(opts, :by_names, false) || Application.get_env(:prestige, :rows_as_maps, false)
    end)
  end
end
