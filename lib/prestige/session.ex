defmodule Prestige.Session do
  @moduledoc """
  Struct that hold connection information as well and current transactions and prepared statements.
  Needed for all interactions with presto

  Example

  ```
  session = Presto.new_session(url: "http://localhost:8080", user: "bbalser", catalog: "hive", schema: "default")
  ```
  """
  @enforce_keys [:url, :user]

  @type t :: %Prestige.Session{
          url: String.t(),
          user: String.t(),
          transaction_id: String.t(),
          catalog: String.t(),
          schema: String.t(),
          prepared_statements: list(String.t())
        }

  defstruct url: nil,
            user: nil,
            transaction_id: nil,
            catalog: nil,
            schema: nil,
            prepared_statements: [],
            receive_timeout: nil

  def new(opts) do
    struct!(__MODULE__, opts)
  end

  def set_transaction_id(%__MODULE__{} = session, transaction_id) do
    %{session | transaction_id: transaction_id}
  end

  def add_prepared_statement(%__MODULE__{} = session, prepared_statement) do
    %{session | prepared_statements: session.prepared_statements ++ [prepared_statement]}
  end

  def remove_prepared_statement(%__MODULE__{prepared_statements: prepared_statements} = session, name) do
    new_prepared_statements =
      prepared_statements
      |> Enum.filter(fn stmt ->
        [ps_name, _] = String.split(stmt, "=", parts: 2)
        ps_name != name
      end)

    %{session | prepared_statements: new_prepared_statements}
  end
end
