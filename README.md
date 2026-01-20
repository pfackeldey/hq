# hq (hep-queue)

A simple task-queue-like implementation with a HTTP server.

Contrary to dask-distributed-like systems workers connect to the HTTP server and fetch work continously until nothing is left. 
The server does not keep track of worker nor does the server push tasks onto them. 
This should reduce a lot of networking overhead.

Tasks are pickled with cloudpickle and distributed as messages.
The server internally holds a queue of those tasks and allows workers to fetch them in FIFO manner.

The HTTP server can be viewed as a very simplified message queue (like RabbitMQ).

## Setup in 3 steps:

Uses [`bun`](https://bun.com) and [`uv`](https://docs.astral.sh/uv/).

1. start the queue server with `bun`:

```shell
bun run typescript/server.ts
```

2. submit some tasks with `uv`:

```shell
uv run example/simple/client.py
```

3. start a worker to consume those tasks with `uv`:

```shell
uv run example/simple/worker.py
```

Once all tasks are finished the server and the worker(s) can be shut down with ctrl+c.
