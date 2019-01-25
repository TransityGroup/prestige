ExUnit.start()
Application.ensure_all_started(:bypass)

Mox.defmock(Prestige.Tesla.Mock, for: Tesla.Adapter)
