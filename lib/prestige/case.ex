defmodule Prestige.Case do
  @moduledoc "TODO"
  use ExUnit.CaseTemplate

  using do
    quote do
      import Prestige.Case, only: [session: 1, table: 1]
    end
  end

  defmacro session(opts) do
    quote location: :keep do
      setup do
        no_tx_session = Prestige.new_session(unquote(opts))
        session = Prestige.PrestoClient.start_transaction(no_tx_session)

        on_exit(fn ->
          Prestige.PrestoClient.rollback(session)
        end)

        [no_tx_session: no_tx_session, session: session]
      end
    end
  end

  defmacro table(table) do
    quote location: :keep do
      setup %{no_tx_session: session} do
        Prestige.query!(session, "CREATE TABLE IF NOT EXISTS #{unquote(table)}")

        :ok
      end
    end
  end
end
