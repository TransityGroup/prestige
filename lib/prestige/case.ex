defmodule Prestige.Case do
  @moduledoc "TODO"
  use ExUnit.CaseTemplate

  using do
    quote do
      import Prestige.Case, only: [session: 1, table: 2]
    end
  end

  defmacro session(opts) do
    quote location: :keep do
      setup_all do
        session = Prestige.new_session(unquote(opts))

        [no_tx_session: session]
      end

      setup %{no_tx_session: no_tx_session} do
        session = Prestige.PrestoClient.start_transaction(no_tx_session)

        on_exit(fn ->
          Prestige.PrestoClient.rollback(session)
        end)

        [session: session]
      end
    end
  end

  defmacro table(name, columns) do
    quote location: :keep do
      setup_all %{no_tx_session: session} do
        column_defs =
          Enum.map(unquote(columns), fn {column_name, column_type} -> "#{column_name} #{column_type}" end)
          |> Enum.join(",")

        Prestige.query!(session, "CREATE TABLE IF NOT EXISTS #{unquote(name)}(#{column_defs})")

        on_exit(fn ->
          Prestige.query!(session, "DROP TABLE IF EXISTS #{unquote(name)}")
        end)

        :ok
      end
    end
  end
end
