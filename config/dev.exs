use Mix.Config

config :prestige, base_url: "https://presto.example.com"

config :husky,
  pre_commit: "mix format --check-formatted && mix credo && mix hex.outdated"
