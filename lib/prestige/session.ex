defmodule Prestige.Session do
  @enforce_keys [:host, :user]
  defstruct [:host, :user]

  def new(opts) do
    struct!(__MODULE__, opts)
  end
end
