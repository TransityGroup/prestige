defmodule Prestige.ColumnDefinition do
  @moduledoc """
  Struct to hold a single column definition
  """

  @type t :: %Prestige.ColumnDefinition{
          name: String.t(),
          type: String.t(),
          sub_type: String.t(),
          sub_columns: list(t)
        }

  defstruct [:name, :type, :sub_type, :sub_columns]

  def new(%{"name" => name, "typeSignature" => type_signature}) do
    transform_column(name, type_signature)
  end

  def new(%__MODULE__{} = column, opts) do
    struct!(column, opts)
  end

  defp transform_column(name, %{"rawType" => "array", "typeArguments" => [type_argument]}) do
    column = %{name: name, type: "array"}
    transform_type(column, type_argument)
  end

  defp transform_column(name, schema) do
    transform_type(%{name: name}, schema)
  end

  defp transform_type(column, %{
         "rawType" => "row",
         "literalArguments" => literal_arguments,
         "typeArguments" => type_arguments
       }) do
    sub_columns =
      Enum.zip(literal_arguments, type_arguments)
      |> Enum.map(fn {name, schema} -> transform_column(name, schema) end)

    fields =
      column
      |> add_type("row")
      |> Map.put(:sub_columns, sub_columns)

    struct!(__MODULE__, fields)
  end

  defp transform_type(column, %{"rawType" => type}) do
    struct!(__MODULE__, add_type(column, type))
  end

  defp add_type(column, type) do
    case Map.has_key?(column, :type) do
      true -> Map.put(column, :sub_type, type)
      false -> Map.put(column, :type, type)
    end
  end
end
