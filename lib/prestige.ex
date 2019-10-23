defmodule Prestige do
  @moduledoc """
  An elixir client for [Prestodb](http://prestodb.github.io/).
  """

  alias Prestige.Client
  alias Prestige.Session

  defmodule Error do
    @moduledoc false

    defexception [:message, :code, :location, :name, :type, :stack]
  end

  defmodule BadRequestError do
    @moduledoc false

    defexception [:message]
  end

  defmodule ConnectionError do
    @moduledoc false

    defexception [:message, :code]
  end

  @type transaction_return :: :commit | {:commit, term} | :rollback | {:rollback, term}

  @spec new_session(keyword) :: Session.t()
  defdelegate new_session(opts), to: Session, as: :new

  @spec prepare(session :: Session.t(), name :: String.t(), statement :: String.t()) ::
          {:ok, Session.t()} | {:error, term}
  def prepare(%Session{} = session, name, statement) do
    new_session = prepare!(session, name, statement)
    {:ok, new_session}
  rescue
    error -> {:error, error}
  end

  @spec prepare!(session :: Session.t(), name :: String.t(), statement :: String.t()) :: Session.t()
  def prepare!(%Session{} = session, name, statement) do
    Client.prepare_statement(session, name, statement)
  end

  @spec execute(session :: Session.t(), name :: String.t(), args :: list) :: {:ok, Prestige.Result.t()} | {:error, term}
  def execute(%Session{} = session, name, args) do
    result = execute!(session, name, args)
    {:ok, result}
  rescue
    error -> {:error, error}
  end

  @spec execute!(session :: Session.t(), name :: String.t(), args :: list) :: Prestige.Result.t()
  def execute!(%Session{} = session, name, args) do
    Client.execute_statement(session, name, args)
    |> Enum.to_list()
    |> collapse_results()
  end

  @spec close(session :: Session.t(), name :: String.t()) :: {:ok, Session.t()} | {:error, term}
  def close(%Session{} = session, name) do
    new_session = close!(session, name)
    {:ok, new_session}
  rescue
    error -> {:error, error}
  end

  @spec close!(session :: Session.t(), name :: String.t()) :: Session.t()
  def close!(%Session{} = session, name) do
    Client.close_statement(session, name)
  end

  @spec query(session :: Session.t(), statement :: String.t(), args :: list) ::
          {:ok, Prestige.Result.t()} | {:error, term}
  def query(%Session{} = session, statement, args \\ []) do
    result = query!(session, statement, args)
    {:ok, result}
  rescue
    error -> {:error, error}
  end

  @spec query!(session :: Session.t(), statement :: String.t(), args :: list) :: Prestige.Result.t()
  def query!(%Session{} = session, statement, args \\ []) do
    Client.execute(session, "stmt", statement, args)
    |> Enum.to_list()
    |> collapse_results()
  end

  @spec stream!(session :: Session.t(), statement :: String.t(), args :: list) :: Enumerable.t()
  def stream!(%Session{} = session, statement, args \\ []) do
    Client.execute(session, "stmt", statement, args)
  end

  @spec transaction(session :: Session.t(), function :: (session :: Session.t() -> transaction_return())) :: term
  def transaction(%Session{} = session, function) when is_function(function, 1) do
    session = Client.start_transaction(session)

    result =
      try do
        function.(session)
      rescue
        e ->
          Client.rollback(session)
          reraise e, __STACKTRACE__
      end

    case result do
      :commit ->
        Client.commit(session)
        :ok

      {:commit, value} ->
        Client.commit(session)
        value

      :rollback ->
        Client.rollback(session)
        :ok

      {:rollback, value} ->
        Client.rollback(session)
        value
    end
  end

  defp collapse_results(results) do
    columns = List.first(results).columns
    rows = flatten(results)

    %Prestige.Result{
      columns: columns,
      rows: rows,
      presto_headers: []
    }
  end

  defp flatten(results) do
    Enum.reduce(results, [], fn result, acc ->
      Enum.reduce(result.rows, acc, fn row, acc ->
        [row | acc]
      end)
    end)
    |> Enum.reverse()
  end
end
