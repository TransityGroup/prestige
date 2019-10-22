defmodule Prestige.PrestoClient.Arguments do
  @moduledoc false

  def to_arg_list(args) do
    args
    |> Enum.map(fn arg ->
      case is_binary(arg) do
        true -> "'" <> arg <> "'"
        false -> to_string(arg)
      end
    end)
    |> Enum.join(",")
  end
end
