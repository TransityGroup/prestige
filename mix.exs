defmodule Prestige.MixProject do
  use Mix.Project

  def project do
    [
      app: :prestige,
      version: "1.0.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      description: description(),
      source_url: "https://github.com/smartcitiesdata/prestige",
      test_paths: test_paths(Mix.env()),
      dialyzer: [
        ignore_warnings: ".dialyzer_ignore.exs",
        flags: [:no_fail_call, :no_return, :no_fun_app],
        plt_file: {:no_warn, ".plt/dialyzer.plt"}
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:tesla, "~> 1.2"},
      {:hackney, "~> 1.15"},
      {:jason, "~> 1.1"},
      {:bypass, "~> 1.0", only: :test},
      {:temporary_env, "~> 2.0", only: :test},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.19", only: :dev},
      {:divo, "~> 1.1.9", only: [:dev, :integration]},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev], runtime: false}
    ]
  end

  defp description do
    "A middleware layer for the Presto database"
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/smartcitiesdata/prestige",
      extras: [
        "README.md"
      ]
    ]
  end

  defp package do
    [
      maintainers: ["smartcitiesdata"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/smartcitiesdata/prestige"}
    ]
  end

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]
end
