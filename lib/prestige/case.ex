defmodule Prestige.Case do
  @moduledoc """
  ExUnit Case that will auto create and drop tables around the entire file and
  create and rollback transactions around each test.

  Example:

  ```
  defmodule Prestige.CaseTest do
    use Prestige.Case

    session url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default"

    table "people", %{
      "name" => "varchar",
      "age" => "int"
    }

    test "transaction that will be rolled back automatically", %{session: session} do
      Prestige.query!(session, "insert into people(name, age) values('Brian', 21)")

      assert [%{"name" => "Brian"}] == Prestige.query!(session, "select name from people") |> Prestige.Result.as_maps()
    end

  end

  ```
  """
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
        session = Prestige.Client.start_transaction(no_tx_session)

        on_exit(fn ->
          Prestige.Client.rollback(session)
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
          try do
            Prestige.query!(session, "DROP TABLE IF EXISTS #{unquote(name)}")
          rescue
            e -> :ok
          end
        end)

        :ok
      end
    end
  end
end
