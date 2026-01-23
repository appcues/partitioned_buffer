defmodule PartitionedBuffer.MixProject do
  use Mix.Project

  @version "0.1.3"
  @source_url "https://github.com/appcues/partitioned_buffer"

  def project do
    [
      app: :partitioned_buffer,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),

      # Testing
      test_coverage: [tool: ExCoveralls, export: "test-coverage"],

      # Dialyzer
      dialyzer: dialyzer(),

      # Hex
      package: package(),
      description: "ETS-based partitioned buffer for high-throughput data processing",

      # Docs
      name: "PartitionedBuffer",
      docs: docs()
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "test.ci": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:nimble_options, "~> 1.0"},
      {:telemetry, "~> 1.0"},

      # Test & Code Analysis
      {:excoveralls, "~> 0.18", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},

      # Docs
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "test.ci": [
        "deps.unlock --check-unused",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "coveralls.html",
        "dialyzer --format short"
      ]
    ]
  end

  defp package do
    [
      name: :partitioned_buffer,
      organization: "appcues",
      links: %{"GitHub" => @source_url},
      files: ~w(lib .formatter.exs mix.exs README* CHANGELOG*)
    ]
  end

  defp docs do
    [
      main: "PartitionedBuffer",
      source_ref: "v#{@version}",
      source_url: @source_url,
      canonical: "http://hexdocs.pm/partitioned_buffer"
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      flags: [
        :error_handling,
        :no_opaque,
        :unknown,
        :no_return
      ]
    ]
  end
end
