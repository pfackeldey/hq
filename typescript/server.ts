const SERVER_NAME = "hq-server";
const SERVER_PORT = Number(process.env.PORT ?? 3000);

// [taskBuf, heavyBuf]
type Payload = [string, string | null];

interface AddTaskReq {
  task: string;
  heavyKey: string | null; // <- pointer/key to the heavyBuf
}

interface AddHeavyReq {
  task: string;
  heavyKey: string; // <- pointer/key to the heavyBuf
}

async function initializeServer() {
  console.log(`${SERVER_NAME} starting...`);

  // initialize states
  var taskId: number = 0;
  const heavy = new Map<string, string>();
  const todo = new Map<number, Payload>();
  const current = new Map<number, Payload>();

  function getTaskById(taskId: number): Payload | null {
    const task = todo.get(taskId);
    if (task === undefined) {
      return null;
    }
    const [taskBuf, heavyKey] = task;
    // evict
    todo.delete(taskId);

    // put them into current Map instead
    current.set(taskId, [taskBuf, heavyKey]);

    // return response
    const heavyBuf = heavyKey === null ? null : heavy.get(heavyKey);
    // can that even happen or is this a type system quirk?
    if (heavyBuf === undefined) {
      return [taskBuf, null];
    }
    return [taskBuf, heavyBuf];
  }

  async function addTask(json: AddTaskReq): Promise<number> {
    const payload = [json.task, json.heavyKey] as Payload;

    console.log(`Received task ${taskId}: ${payload}`);

    // add task to todo map
    todo.set(taskId, payload);
    const thisId = taskId;
    // increment for next task
    taskId++;
    // return id of this task
    return thisId;
  }

  const server = Bun.serve({
    port: SERVER_PORT,

    // `routes` requires Bun v1.2.3+
    routes: {
      // Health check
      "/status": new Response("OK"),

      // Task method handlers
      // put multiple todos at once
      "/tasks": {
        POST: async (req: Request) => {
          const jsons = (await req.json()) as Array<AddTaskReq>;
          const taskIds = [];
          for (const json of jsons) {
            const id = await addTask(json);
            taskIds.push(id);
          }
          return Response.json({ taskIds: taskIds });
        },
      },

      // get next n tasks
      "/tasks/:n": async (req) => {
        var n = Number(req.params.n);
        const payloads = [];
        var it: number = 0;

        // in case we request more tasks than there are
        if (todo.size < n) {
          n = todo.size;
        }

        while (it < n) {
          const _taskId = todo.keys().next().value;
          if (_taskId === undefined) {
            return new Response("No tasks available", { status: 404 });
          }
          const payload = getTaskById(_taskId) as Payload;
          payloads.push(payload);
          it++;
        }

        return Response.json({ payloads: payloads });
      },

      // Heavy method handlers
      "/heavy": {
        POST: async (req: Request) => {
          const json = (await req.json()) as AddHeavyReq;

          if (!json.heavyKey) {
            return Response.json(
              {
                error: `Heavy task key required, can't be empty, got ${json.heavyKey}`,
              },
              { status: 404 },
            );
          }

          console.log(`Received heavy task ${json.heavyKey}: ${json.task}`);

          // add task to todo map
          heavy.set(json.heavyKey, json.task);
          // increment for next task
          return Response.json({ heavyKey: json.heavyKey });
        },
      },

      // Wildcard route for all routes that aren't otherwise matched
      "/tasks/*": Response.json({ message: "Not found" }, { status: 404 }),
      "/*": Response.json({ message: "Not found" }, { status: 404 }),
    },
  });

  console.log(`${SERVER_NAME} running at ${server.url}`);
}

// Initialize the server
initializeServer().catch((error: unknown) => {
  console.log(`Failed to start ${SERVER_NAME}: ${String(error)}`);
  process.exit(1);
});
