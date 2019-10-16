defmodule Prestige.PrestoClient do
  @moduledoc false
  alias Prestige.Session
  alias Prestige.PrestoClient.{RequestStream, ResponseParser}

  @presto_transaction_id "X-Presto-Transaction-Id"
  @presto_started_transaction_id "x-presto-started-transaction-id"

  defmodule Request do
    @moduledoc false
    defstruct [:session, :name, :statement, :args, :headers]
  end

  def execute(session, name, statement, args, headers \\ []) do
    request = %Request{session: session, name: name, statement: statement, args: args, headers: headers}

    RequestStream.stream(request)
    |> ResponseParser.parse()
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
