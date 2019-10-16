defmodule Prestige.Result do
  @moduledoc "TODO"

  defstruct [:columns, :rows, :presto_headers]

  def as_maps(%Prestige.Result{} = result) do
    columns = Enum.map(result.columns, fn col -> col.name end)

    Enum.map(result.rows, fn row ->
      Enum.zip(columns, row) |> Map.new()
    end)
  end
end
