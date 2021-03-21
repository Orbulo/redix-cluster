defmodule RedixCluster.Mixfile do
  use Mix.Project

  def project do
    [
      app: :redix_cluster_remastered,
      version: "1.2.1",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "RedixCluster",
      source_url: "https://github.com/Orbulo/redix-cluster",
      elixirc_paths: elixirc_paths(Mix.env()),
      # The main page in the docs
      docs: [main: "RedixCluster", extras: ["README.md"]],
      dialyzer: [plt_add_deps: :project]
    ]
  end

  def application do
    [
      extra_applications: applications(Mix.env())
    ]
  end

  defp applications(:dev), do: applications(:all) ++ [:remixed_remix]
  defp applications(_all), do: [:logger, :runtime_tools]

  defp deps do
    [
      {:redix, ">= 0.0.0"},
      {:poolboy, "~> 1.5"},
      # Necessary for dev and test
      {:ex_doc, "~> 0.18.0", only: :dev},
      {:credo, ">= 0.3.0", only: [:dev, :test]},
      {:remixed_remix, "~> 1.0.0", only: :dev}
    ]
  end

  defp package do
    [
      description:
        "A client for managing the connection to a Redis Cluster using Redix as a client.",
      # These are the default files included in the package
      files: ["lib", "mix.exs", "README.md"],
      maintainers: ["Coby Benveniste"],
      links: %{"GitHub" => "https://github.com/SpotIM/redix-cluster"},
      licenses: ["MIT License"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
