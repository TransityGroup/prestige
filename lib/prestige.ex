defmodule Prestige do
  @moduledoc """
  An elixir client for [Prestodb](http://prestodb.github.io/).
  """

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
end
