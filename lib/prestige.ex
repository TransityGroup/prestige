defmodule Prestige do
  defdelegate execute(statement, opts \\ []), to: Prestige.Statement

  defdelegate prefetch(result), to: Prestige.Statement
end
