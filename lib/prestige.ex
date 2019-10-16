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

  @doc """
  Executes a prepared statement against presto, returns a stream

  Options:

    * `:rows_as_maps` - converts response to a list of maps, with the column name as the key and the row data as the value

  All other specified options are passed directly to presto as headers, a
  full list of those headers can be found [here](https://github.com/prestosql/presto/blob/master/presto-client/src/main/java/io/prestosql/client/PrestoHeaders.java).

  ## Examples

      iex> Prestige.execute("select * from users") |> Prestige.prefetch
      [[1, "Brian"], [2, "Shannon"]]

      iex> Prestige.execute("select * from users", rows_as_maps: true) |> Prestige.prefetch
      [%{"id" => 1, "name" => "Brian"}, %{"id" => 2, "name" => "Shannon"}]
  """
  defdelegate execute(statement, opts \\ []), to: Prestige.Statement

  @doc """
  Converts a presto stream into a map for consumption
  """
  defdelegate prefetch(result), to: Prestige.Statement

  defdelegate new_session(opts), to: Prestige.Session, as: :new

  def query(%Session{} = session, statement, args \\ []) do
    result =
      PrestoClient.execute(session, "stmt", statement, args)
      |> Enum.to_list()
      |> collapse_results()

    {:ok, result}
  end

  def query!(%Session{} = session, statement, args \\ []) do
    case query(session, statement, args) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  def stream!(%Session{} = session, statement, args \\ []) do
    PrestoClient.execute(session, "stmt", statement, args)
  end

  def transaction(%Session{} = session, function) when is_function(function, 1) do
    session = PrestoClient.start_transaction(session)

    case function.(session) do
      :rollback -> PrestoClient.rollback(session)
      :commit -> PrestoClient.commit(session)
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
