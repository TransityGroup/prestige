defmodule Prestige do
  @moduledoc """
  An elixir client for [Prestodb](http://prestodb.github.io/).
  """

  defmodule Error do
    defexception [:message, :code, :location, :name, :type, :stack]
  end

  defmodule BadRequestError do
    defexception [:message]
  end

  defdelegate execute(statement, opts \\ []), to: Prestige.Statement

  defdelegate prefetch(result), to: Prestige.Statement
end
