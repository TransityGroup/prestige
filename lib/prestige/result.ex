defmodule Prestige.Result do
  def transform({:ok, %Tesla.Env{status: 200, body: body}}, rows_as_maps) do
    data =
      Map.get(body, "data", [])
      |> Enum.map(&transform_row(&1, Map.get(body, "columns", []), rows_as_maps))

    {data, %{next_uri: body["nextUri"], rows_as_maps: rows_as_maps}}
  end

  defp transform_row(row, _columns, false), do: row

  defp transform_row(row, columns, true) do
    columns
    |> Enum.zip(row)
    |> Enum.map(fn {schema, value} -> %{name: schema["name"], schema: schema["typeSignature"], value: value} end)
    |> Enum.map(&transform_column/1)
    |> Map.new()
  end

  defp transform_column(%{
         name: name,
         schema: %{"rawType" => "array", "typeArguments" => [type_argument]},
         value: values
       }) do
    transformed_values =
      values
      |> Enum.map(fn value -> %{schema: type_argument, value: value} end)
      |> Enum.map(&transform_value/1)

    {name, transformed_values}
  end

  defp transform_column(column) do
    {column[:name], transform_value(column)}
  end

  defp transform_value(%{
         schema: %{"rawType" => "row", "literalArguments" => literal_arguments, "typeArguments" => type_arguments},
         value: values
       }) do
    Enum.zip([literal_arguments, type_arguments, values])
    |> Enum.map(fn {name, schema, value} -> %{name: name, schema: schema, value: value} end)
    |> Enum.map(&transform_column/1)
    |> Map.new()
  end

  defp transform_value(%{value: value}), do: value
end
