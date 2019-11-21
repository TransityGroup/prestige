defmodule Prestige.Client.RequestStream do
  @moduledoc false
  use Tesla

  alias Prestige.Client.{Arguments, Request}

  plug Tesla.Middleware.Headers, [{"content-type", "text/plain"}]
  plug Prestige.Middleware.Retry, delay: 100, max_retries: 5
  plug Tesla.Middleware.Logger, log_level: :debug
  plug Tesla.Middleware.DecodeJson

  adapter(Tesla.Adapter.Hackney)

  @statement_path "/v1/statement"

  defmodule NextUri do
    @moduledoc false
    defstruct [:uri, :session]

    def new(body, session) do
      %__MODULE__{uri: Map.get(body, "nextUri"), session: session}
    end
  end

  def stream(%Request{} = request) do
    Stream.resource(
      fn -> request end,
      &make_request/1,
      fn _ -> :ok end
    )
  end

  defp make_request(%Request{session: session, prepare_statement: false} = request) do
    url = session.url <> @statement_path
    headers = create_headers(session, nil, request.headers)

    post(url, request.statement, headers: headers, opts: custom_opts(session))
    |> validate(session)
  end

  defp make_request(%Request{session: session} = request) do
    url = session.url <> @statement_path
    prepared_statement = request.name <> "=" <> URI.encode_www_form(request.statement)
    execute_statement = execute_statement(request.name, request.args)
    headers = create_headers(session, prepared_statement, request.headers)

    post(url, execute_statement, headers: headers, opts: custom_opts(session))
    |> validate(session)
  end

  defp make_request(%NextUri{uri: nil}) do
    {:halt, :ok}
  end

  defp make_request(%NextUri{uri: uri, session: session}) do
    get(uri, opts: custom_opts(session))
    |> validate(session)
  end

  defp validate({:ok, %Tesla.Env{status: 200, body: body} = response}, session) do
    {[response], NextUri.new(body, session)}
  end

  defp validate({:ok, %Tesla.Env{status: 400, body: body}}, _session) do
    raise Prestige.BadRequestError, message: body
  end

  defp validate({:error, :econnrefused}, _session) do
    raise Prestige.ConnectionError, message: "Error connecting to Presto.", code: :econnrefused
  end

  defp execute_statement(name, []) do
    "EXECUTE #{name}"
  end

  defp execute_statement(name, args) do
    "EXECUTE #{name} USING #{Arguments.to_arg_list(args)}"
  end

  defp create_headers(session, prepared_statement, custom_headers) do
    [
      header("X-Presto-User", session.user),
      prepared_statement_header(session, prepared_statement),
      header("X-Presto-Catalog", session.catalog),
      header("X-Presto-Schema", session.schema),
      header("X-Presto-Transaction-Id", session.transaction_id)
      | custom_headers
    ]
    |> List.flatten()
  end

  defp custom_opts(session) do
    case session.receive_timeout do
      nil -> []
      timeout -> [adapter: [recv_timeout: timeout]]
    end
  end

  defp header(_key, nil) do
    []
  end

  defp header(key, value) do
    {key, value}
  end

  defp prepared_statement_header(%{prepared_statements: []}, nil) do
    []
  end

  defp prepared_statement_header(session, nil) do
    header("X-Presto-Prepared-Statement", Enum.join(session.prepared_statements, ","))
  end

  defp prepared_statement_header(session, prepared_statement) do
    header("X-Presto-Prepared-Statement", Enum.join([prepared_statement | session.prepared_statements], ","))
  end
end
