defmodule Prestige do
  @moduledoc """
  An elixir client for [Prestodb](http://prestodb.github.io/).
  """

  alias Prestige.PrestoClient
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

  defdelegate new_session(opts), to: Prestige.Session, as: :new

  def query(%Session{} = session, statement, args \\ []) do
    result = query!(session, statement, args)
    {:ok, result}
  rescue
    error -> {:error, error}
  end

  def query!(%Session{} = session, statement, args \\ []) do
    PrestoClient.execute(session, "stmt", statement, args)
    |> Enum.to_list()
    |> collapse_results()
  end

  def stream!(%Session{} = session, statement, args \\ []) do
    PrestoClient.execute(session, "stmt", statement, args)
  end

  def transaction(%Session{} = session, function) when is_function(function, 1) do
    session = PrestoClient.start_transaction(session)

    case function.(session) do
      :commit ->
        PrestoClient.commit(session)
        :ok

      {:commit, value} ->
        PrestoClient.commit(session)
        value

      :rollback ->
        PrestoClient.rollback(session)
        :ok

      {:rollback, value} ->
        PrestoClient.rollback(session)
        value
    end
  end

  defp collapse_results([]), do: []

  defp collapse_results(results) do
    columns = List.first(results).columns
    rows = flatten(results)

    %Prestige.Result{
      columns: columns,
      rows: rows
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
