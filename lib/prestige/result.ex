defmodule Prestige.Result do
  @moduledoc """
  Handles transforming result from presto into desired datastructure
  """

  @doc """
  Transforms a successful presto select query into a map
  """
  def transform({:ok, %Tesla.Env{status: 200, body: %{"error" => error}}}, _rows_as_maps) do
    raise Prestige.Error,
      message: error["message"],
      code: error["errorCode"],
      location: error["errorLocation"],
      name: error["errorName"],
      type: error["errorType"],
      stack: get_in(error, ["failureInfo", "stack"])
  end

  def transform({:ok, %Tesla.Env{status: 200, body: body}}, rows_as_maps) do
    data =
      body
      |> Map.get("data", [])
      |> Enum.map(&transform_row(&1, Map.get(body, "columns", []), rows_as_maps))

    {data, %{next_uri: body["nextUri"], rows_as_maps: rows_as_maps}}
  end

  def transform({:ok, %Tesla.Env{status: 400, body: body}}, _rows_as_maps) do
    raise Prestige.BadRequestError, message: body
  end

  def transform({:error, :econnrefused}, _) do
    raise Prestige.ConnectionError, message: "Error connecting to Presto.", code: :econnrefused
  end

  defp transform_row(row, _columns, false), do: row

  defp transform_row(row, columns, true) do
    columns
    |> Enum.zip(row)
    |> Enum.map(fn {schema, value} -> %{name: schema["name"], schema: schema["typeSignature"], value: value} end)
    |> Enum.map(&transform_column/1)
    |> Map.new()
  end

  defp transform_column(%{schema: %{"rawType" => "array"}, value: nil} = column) do
    column
    |> Map.update(:value, [], fn _ -> [] end)
    |> transform_column()
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
         schema: %{"rawType" => "row", "literalArguments" => _, "typeArguments" => _},
         value: nil
       }),
       do: nil

  defp transform_value(%{
         schema: %{"rawType" => "row", "literalArguments" => literal_arguments, "typeArguments" => type_arguments},
         value: values
       }) do
    [literal_arguments, type_arguments, values]
    |> Enum.zip()
    |> Enum.map(fn {name, schema, value} -> %{name: name, schema: schema, value: value} end)
    |> Enum.map(&transform_column/1)
    |> Map.new()
  end

  defp transform_value(%{value: value}), do: value
end
