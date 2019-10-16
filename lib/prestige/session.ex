defmodule Prestige.Session do
  @enforce_keys [:host, :user]
  defstruct [:host, :user, :transaction_id]

  def new(opts) do
    struct!(__MODULE__, opts)
  end

  def set_transaction_id(%__MODULE__{} = session, transaction_id) do
    %{session | transaction_id: transaction_id}
  end
end
