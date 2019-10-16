defmodule Prestige.Session do
  @moduledoc "TODO"
  @enforce_keys [:url, :user]
  defstruct [:url, :user, :transaction_id, :catalog, :schema]

  def new(opts) do
    struct!(__MODULE__, opts)
  end

  def set_transaction_id(%__MODULE__{} = session, transaction_id) do
    %{session | transaction_id: transaction_id}
  end
end
