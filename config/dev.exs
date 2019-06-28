use Mix.Config

config :prestige, base_url: "https://presto.example.com"

config :husky,
  pre_commit: "./scripts/git_pre_commit_hook.sh"
