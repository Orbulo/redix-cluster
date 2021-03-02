defmodule RedixCluster do
  @moduledoc """
  ## RedixCluster

  The main API to interface with a Redis Cluster using Redix as a client.

  **NOTE: When using Redis, make sure CROSSSLOT Keys in request hash to the same slot.**
  """

  use Supervisor

  @type command :: [binary]
  @type conn :: module | atom | pid

  @max_retry 1_000
  @redis_retry_delay 100

  @doc """
    Starts RedixCluster as a supervisor in your supervision tree.
  """
  @spec start_link(opts :: Keyword.t()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    {conn_name, _opts} = Keyword.pop(opts, :conn_name, RedixCluster)
    name = Module.concat(conn_name, Main)
    Supervisor.start_link(__MODULE__, conn_name, name: name)
  end

  @spec init(conn_name :: conn) :: Supervisor.on_start()
  def init(conn_name) do
    children = [
      %{
        id: Module.concat(conn_name, Pool),
        start: {RedixCluster.Pool, :start_link, [[conn_name: conn_name]]},
        type: :supervisor
      },
      %{
        id: Module.concat(conn_name, RedixCluster.Monitor),
        start: {RedixCluster.Monitor, :start_link, [[conn_name: conn_name]]},
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  `command/3`

  Runs a command on the Redis cluster.
  """
  @spec command(conn, command, Keyword.t()) ::
          {:ok, Redix.Protocol.redis_value()} | {:error, Redix.Error.t() | atom}
  def command(conn, command, opts \\ []), do: command(conn, command, opts, 0, 0)

  @doc """
  `pipeline/3`

  Runs a pipeline on the Redis cluster.
  """
  @spec pipeline(conn, [command], Keyword.t()) ::
          {:ok, [Redix.Protocol.redis_value()]} | {:error, atom}
  def pipeline(conn, commands, opts \\ []), do: pipeline(conn, commands, opts, 0, 0)

  defp command(_conn, _command, _opts, count, _delay) when count >= @max_retry,
    do: {:error, :no_connection}

  defp command(conn, command, opts, count, delay) do
    Process.sleep(delay)

    conn
    |> RedixCluster.Run.command(command, opts)
    |> need_retry(conn, command, opts, count, delay, :command)
  end

  defp pipeline(_conn, _commands, _opts, count, _delay) when count >= @max_retry,
    do: {:error, :no_connection}

  defp pipeline(conn, commands, opts, count, delay) do
    Process.sleep(delay)

    conn
    |> RedixCluster.Run.pipeline(commands, opts)
    |> need_retry(conn, commands, opts, count, delay, :pipeline)
  end

  defp need_retry({:error, :retry}, conn, command, opts, count, delay, :command),
    do: command(conn, command, opts, count + 1, delay + @redis_retry_delay)

  defp need_retry({:error, :retry}, conn, commands, opts, count, delay, :pipeline),
    do: pipeline(conn, commands, opts, count + 1, delay + @redis_retry_delay)

  defp need_retry(result, _conn, _command, _count, _delay, _opts, _type), do: result
end
