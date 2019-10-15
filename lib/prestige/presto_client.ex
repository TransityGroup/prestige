defmodule Prestige.PrestoClient do
  use Tesla

  plug Tesla.Middleware.Headers, [{"content-type", "text/plain"}]
  plug Tesla.Middleware.Logger, log_level: :debug
  plug Tesla.Middleware.DecodeJson

  defmodule Request do
    defstruct [:session, :name, :statement, :args]
  end

  defmodule NextRequest do
    defstruct [:uri]
  end

  def execute(session, name, statement, args) do
    request = %Request{session: session, name: name, statement: statement, args: args}

    Stream.resource(
      fn -> request end,
      &next/1,
      fn _ -> :ok end
    )
  end

  defp next(%Request{session: session} = request) do
    url = session.host <> "/v1/statement"
    prepared_statement = request.name <> "=" <> URI.encode_www_form(request.statement)
    execute_statement = execute_statement(request.name, request.args)
    headers = create_headers(session, prepared_statement)

    post(url, execute_statement, headers: headers)
    |> parse_response()
  end

  defp next(%NextRequest{uri: nil}), do: {:halt, :ok}

  defp next(%NextRequest{uri: next_uri}) do
    get(next_uri)
    |> parse_response()
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

  defp create_headers(session, prepared_statement) do
    [
      {"X-Presto-User", session.user},
      {"X-Presto-Prepared-Statement", prepared_statement}
    ]
  end

  defp parse_response({:ok, %Tesla.Env{status: 200, body: body}}) do
    next = %NextRequest{uri: Map.get(body, "nextUri")}

    case Map.get(body, "data", []) do
      [] ->
        {[], next}

      data ->
        result = %Prestige.Result{
          columns: Map.get(body, "columns", []) |> transform_columns(),
          rows: data
        }

        {[result], next}
    end
  end

  defp transform_columns(columns) do
    Enum.map(columns, fn %{"name" => name, "type" => type} ->
      %Prestige.ColumnDefinition{name: name, type: type}
    end)
  end
end
