defmodule Prestige.Client do
  @moduledoc false
  alias Prestige.Session
  alias Prestige.Client.{Arguments, RequestStream, ResponseParser}

  @presto_transaction_id "X-Presto-Transaction-Id"
  @presto_started_transaction_id "x-presto-started-transaction-id"
  @presto_added_prepare "x-presto-added-prepare"

  defmodule Request do
    @moduledoc false
    defstruct session: nil,
              name: nil,
              statement: nil,
              args: nil,
              headers: [],
              prepare_statement: true
  end

  @spec execute(session :: Session.t(), name :: String.t(), statement :: String.t(), args :: list, headers :: list) ::
          Enumerable.t()
  def execute(session, name, statement, args, headers \\ []) do
    request = %Request{session: session, name: name, statement: statement, args: args, headers: headers}

    RequestStream.stream(request)
    |> ResponseParser.parse()
  end

  def prepare_statement(session, name, statement, headers \\ []) do
    prepare_statement = "PREPARE #{name} FROM #{statement}"

    request = %Request{
      session: session,
      name: name,
      statement: prepare_statement,
      prepare_statement: false,
      headers: headers
    }

    [result] =
      RequestStream.stream(request)
      |> ResponseParser.parse()
      |> Enum.to_list()

    prepared_header = get_header(result.presto_headers, @presto_added_prepare)
    Session.add_prepared_statement(session, prepared_header)
  end

  @spec execute_statement(session :: Session.t(), name :: String.t(), args :: list, headers :: list) :: Enumerable.t()
  def execute_statement(session, name, args, headers \\ []) do
    execute_statement = "EXECUTE #{name} USING #{Arguments.to_arg_list(args)}"

    request = %Request{
      session: session,
      name: name,
      statement: execute_statement,
      prepare_statement: false,
      headers: headers
    }

    RequestStream.stream(request)
    |> ResponseParser.parse()
  end

  def close_statement(session, name) do
    deallocate_statement = "DEALLOCATE PREPARE #{name}"

    request = %Request{
      session: session,
      statement: deallocate_statement,
      prepare_statement: false
    }

    RequestStream.stream(request)
    |> ResponseParser.parse()
    |> Stream.run()

    Session.remove_prepared_statement(session, name)
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

  defp get_header(headers, name) do
    case Enum.find(headers, fn {key, _value} -> key == name end) do
      {_key, value} -> value
      nil -> nil
    end
  end
end
