defmodule Prestige.MixProject do
  use Mix.Project

  def project do
    [
      app: :prestige,
      version: "0.3.1",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs(),
      description: description(),
      source_url: "https://github.com/smartcitiesdata/prestige"
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
      {:credo, "~> 0.10", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.19", only: :dev}
    ]
  end

  defp description do
    "A Elixir client for Prestodb"
  end

  defp docs do
    [
      main: "README",
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
end
