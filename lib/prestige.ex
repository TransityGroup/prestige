defmodule Prestige do
  @moduledoc """
  An elixir client for [Prestodb](http://prestodb.github.io/).
  """
  defdelegate execute(statement, opts \\ []), to: Prestige.Statement

  defdelegate prefetch(result), to: Prestige.Statement
end
