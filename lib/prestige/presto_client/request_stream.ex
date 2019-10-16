defmodule Prestige.PrestoClient.RequestStream do
  @moduledoc false
  use Tesla

  alias Prestige.PrestoClient.Request

  plug Tesla.Middleware.Headers, [{"content-type", "text/plain"}]
  plug Tesla.Middleware.Logger, log_level: :debug
  plug Tesla.Middleware.DecodeJson

  defmodule NextUri do
    @moduledoc false
    defstruct [:uri]

    def new(body) do
      %__MODULE__{uri: Map.get(body, "nextUri")}
    end
  end

  def stream(%Request{} = request) do
    Stream.resource(
      fn -> request end,
      &make_request/1,
      fn _ -> :ok end
    )
  end

  defp make_request(%Request{session: session} = request) do
    url = session.url <> "/v1/statement"
    prepared_statement = request.name <> "=" <> URI.encode_www_form(request.statement)
    execute_statement = execute_statement(request.name, request.args)
    headers = create_headers(session, prepared_statement, request.headers)

    post(url, execute_statement, headers: headers)
    |> validate()
  end

  defp make_request(%NextUri{uri: nil}) do
    {:halt, :ok}
  end

  defp make_request(%NextUri{uri: uri}) do
    get(uri)
    |> validate()
  end

  defp validate({:ok, %Tesla.Env{status: 200, body: body} = response}) do
    {[response], NextUri.new(body)}
  end

  defp validate({:ok, %Tesla.Env{status: 400, body: body}}) do
    raise Prestige.BadRequestError, message: body
  end

  defp validate({:error, :econnrefused}) do
    raise Prestige.ConnectionError, message: "Error connecting to Presto.", code: :econnrefused
  end

  defp execute_statement(name, []) do
    "EXECUTE #{name}"
  end

  defp execute_statement(name, args) do
    "EXECUTE #{name} USING #{to_arg_list(args)}"
  end

  defp create_headers(session, prepared_statement, custom_headers) do
    [
      header("X-Presto-User", session.user),
      header("X-Presto-Prepared-Statement", prepared_statement),
      header("X-Presto-Catalog", session.catalog),
      header("X-Presto-Schema", session.schema),
      header("X-Presto-Transaction-Id", session.transaction_id)
      | custom_headers
    ]
    |> List.flatten()
  end

  defp header(_key, nil) do
    []
  end

  defp header(key, value) do
    {key, value}
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
end
