defmodule Prestige.PrestoClient do
  use Tesla

  alias Prestige.Session

  @presto_transaction_id "X-Presto-Transaction-Id"
  @presto_started_transaction_id "x-presto-started-transaction-id"

  plug Tesla.Middleware.Headers, [{"content-type", "text/plain"}]
  plug Tesla.Middleware.Logger, log_level: :debug
  plug Tesla.Middleware.DecodeJson

  defmodule Request do
    defstruct [:session, :name, :statement, :args, :headers]
  end

  defmodule NextRequest do
    @presto_prefix "x-presto"

    defstruct [:uri, :presto_headers]

    def new() do
      %__MODULE__{presto_headers: []}
    end

    def new(uri) do
      %__MODULE__{uri: uri, presto_headers: []}
    end

    def new(%__MODULE__{} = last, uri, headers) do
      %__MODULE__{uri: uri, presto_headers: merge_presto_headers(last, headers)}
    end

    defp merge_presto_headers(%__MODULE__{} = last, headers) do
      last.presto_headers ++
        Enum.filter(headers, fn {name, _value} -> String.starts_with?(name, @presto_prefix) end)
    end
  end

  def execute(session, name, statement, args, headers \\ []) do
    request = %Request{session: session, name: name, statement: statement, args: args, headers: headers}

    Stream.resource(
      fn -> request end,
      &next/1,
      fn _ -> :ok end
    )
  end

  def start_transaction(session) do
    [result] = execute(session, "stmt", "START TRANSACTION", [], [{@presto_transaction_id, "none"}]) |> Enum.to_list()
    transaction_id = get_header(result.presto_headers, @presto_started_transaction_id)
    Session.set_transaction_id(session, transaction_id)
  end

  def rollback(session) do
    execute(session, "stmt", "ROLLBACK", []) |> Enum.to_list()
  end

  def commit(session) do
    execute(session, "stmt", "COMMIT", []) |> Enum.to_list()
  end

  defp next(%Request{session: session} = request) do
    url = session.host <> "/v1/statement"
    prepared_statement = request.name <> "=" <> URI.encode_www_form(request.statement)
    execute_statement = execute_statement(request.name, request.args)
    headers = create_headers(session, prepared_statement, request.headers)

    post(url, execute_statement, headers: headers)
    |> parse_response(NextRequest.new())
  end

  defp next(%NextRequest{uri: nil}), do: {:halt, :ok}

  defp next(%NextRequest{uri: next_uri} = next_request) do
    get(next_uri)
    |> parse_response(next_request)
  end

  defp execute_statement(name, []) do
    "EXECUTE #{name}"
  end

  defp execute_statement(name, args) do
    "EXECUTE #{name} USING #{to_arg_list(args)}"
  end

  defp to_arg_list(args) do
    args
    |> Enum.map(fn arg ->
      case is_binary(arg) do
        true -> "'" <> arg <> "'"
        false -> to_string(arg)
      end
    end)
    |> Enum.join(",")
  end

  defp create_headers(session, prepared_statement, custom_headers) do
    transaction_header =
      case session.transaction_id do
        nil ->
          []

        value ->
          [{@presto_transaction_id, value}]
      end

    [
      {"X-Presto-User", session.user},
      {"X-Presto-Prepared-Statement", prepared_statement},
      transaction_header
    ] ++ custom_headers
    |> List.flatten()
  end

  defp parse_response({:ok, %Tesla.Env{status: 200, body: body, headers: headers}}, %NextRequest{} = last_request) do
    next_request = NextRequest.new(last_request, Map.get(body, "nextUri"), headers)

    case Map.get(body, "data", []) do
      [] ->
        {[], next_request}

      data ->
        result = %Prestige.Result{
          columns: Map.get(body, "columns", []) |> transform_columns(),
          rows: data,
          presto_headers: next_request.presto_headers
        }

        {[result], next_request}
    end
  end

  defp transform_columns(columns) do
    Enum.map(columns, fn %{"name" => name, "type" => type} ->
      %Prestige.ColumnDefinition{name: name, type: type}
    end)
  end

  defp get_header(headers, name) do
    case Enum.find(headers, fn {key, _value} -> key == name end) do
      {_key, value} -> value
      nil -> nil
    end
  end
end
