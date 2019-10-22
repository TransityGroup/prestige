defmodule Prestige.Result do
  @moduledoc """
  Struct to hold query results.
  """

  alias Prestige.ColumnDefinition

  @type t :: %Prestige.Result{
          columns: list(ColumnDefinition.t()),
          rows: list,
          presto_headers: list
        }

  defstruct [:columns, :rows, :presto_headers]

  @spec as_maps(t) :: list(%{String.t() => term})
  def as_maps(%Prestige.Result{} = result) do
    Enum.map(result.rows, &transform_row(&1, result.columns))
  end

  defp transform_row(row, columns) do
    columns
    |> Enum.zip(row)
    |> Enum.map(fn {column, value} -> transform_column(column, value) end)
    |> Map.new()
  end

  defp transform_column(%{type: "array"} = column, nil) do
    transform_column(column, [])
  end

  defp transform_column(%{type: "array"} = column, values) do
    sub_definition = ColumnDefinition.new(column, type: column.sub_type)

    transformed_values =
      values
      |> Enum.map(&transform_value(sub_definition, &1))

    {column.name, transformed_values}
  end

  defp transform_column(column, value) do
    {column.name, transform_value(column, value)}
  end

  defp transform_value(%{type: "row"}, nil), do: nil

  defp transform_value(%{type: "row"} = column, values) do
    [column.sub_columns, values]
    |> Enum.zip()
    |> Enum.map(fn {sub_column, value} -> transform_column(sub_column, value) end)
    |> Map.new()
  end

  defp transform_value(_column, value), do: value
end
