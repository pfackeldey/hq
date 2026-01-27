const SERVER_NAME = "hq-server";
const SERVER_PORT = Number(process.env.PORT ?? 3000);
const WORKER_TIMEOUT = Number(process.env.WORKER_TIMEOUT ?? 30_000); // in milliseconds, default: 30s

// payload is a tuple type of [taskBuf, heavyBuf]
type Payload = [string, string | null];

// workerId
type WorkerId = string;

// taskId
type TaskId = number;

// array of taskIds
type TaskIds = TaskId[];

interface AddTaskReq {
  task: string;
  heavyKey: string | null; // <- optional pointer/key to the heavyBuf
}

interface AddHeavyReq {
  task: string;
  heavyKey: string; // <- pointer/key to the heavyBuf
}

interface TaskStatus {
  status: "success" | "running" | "error" | "queued" | "lost";
  info: string | null; // additional info, e.g. path to log file / error (something a worker needs to specify)
}

interface UpdateTaskStatusReq {
  workerId: WorkerId;
  taskStatus: TaskStatus;
}

// function that starts a callback periodically every 'delay' ms
async function periodicCallback(callback: Function, delay: number) {
  const sleep = (delay: number) => {
    return new Promise((done, _) => setTimeout(done, delay));
  };

  while (true) {
    callback();
    await sleep(delay);
  }
}

async function initializeServer() {
  console.log(`${SERVER_NAME} starting...`);

  // initialize states
  var taskId: TaskId = 0; // counter for all tasks to generate unique IDs
  const heavy = new Map<string, string>();
  const available = new Map<TaskId, Payload>();
  const running = new Map<WorkerId, TaskIds>();
  const tasksStatus = new Map<TaskId, TaskStatus>();
  const workersStatus = new Map<WorkerId, number>();

  function getTaskById(workerId: WorkerId, taskId: TaskId): Payload | null {
    const task = available.get(taskId);
    if (task === undefined) {
      return null;
    }
    const [taskBuf, heavyKey] = task;
    // evict from available Map
    available.delete(taskId);

    // put them into running Map instead
    const runningTasks = running.get(workerId);
    if (runningTasks) {
      runningTasks.push(taskId);
    } else {
      running.set(workerId, [taskId]);
    }

    // update status
    tasksStatus.set(taskId, { status: "running", info: null });

    // return response
    const heavyBuf = heavyKey === null ? null : heavy.get(heavyKey);
    // can that even happen or is this a type system quirk?
    if (heavyBuf) {
      return [taskBuf, heavyBuf];
    }
    return [taskBuf, null];
  }

  async function addTask(json: AddTaskReq): Promise<TaskId> {
    const payload = [json.task, json.heavyKey] as Payload;

    console.log(`Received task ${taskId}: ${payload}`);

    // add task to available map
    available.set(taskId, payload);
    // update it's status
    tasksStatus.set(taskId, { status: "queued", info: null });
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
      // Health check of the server
      "/status": new Response("OK"),

      // Worker heatbeat
      "/status/:workerId": async (req) => {
        const workerId = req.params.workerId as WorkerId;
        workersStatus.set(workerId, Date.now());
        return new Response("OK");
      },

      // Task method handlers
      // put multiple availables at once
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
      "/tasks/fetch/:worker_id/:n": async (req) => {
        const workerId = req.params.worker_id as WorkerId;
        var n = Number(req.params.n);
        const payloads = [];
        const taskIds = [];
        var it: number = 0;

        // in case we request more tasks than there are
        if (available.size < n) {
          n = available.size;
        }

        while (it < n) {
          const id = available.keys().next().value;
          if (id === undefined) {
            return new Response("No tasks available", { status: 404 });
          }
          const payload = getTaskById(workerId, id) as Payload;
          payloads.push(payload);
          taskIds.push(id);
          it++;
        }

        return Response.json({ taskIds: taskIds, payloads: payloads });
      },

      "/tasks/status/:id": {
        // get task status
        GET: async (req) => {
          const id = Number(req.params.id);

          // check if task is finished
          const taskStatus = tasksStatus.get(id) as TaskStatus;

          // if not, check if it's still running, else error
          if (taskStatus) {
            // return status if task is finished
            return Response.json({
              status: taskStatus.status,
              info: taskStatus.info,
            });
          }
          return new Response(
            `Task ${id} doesn't exist, can't query its status`,
            { status: 404 },
          );
        },
        // update task status when task finished successfully or with failure
        POST: async (req) => {
          const id = Number(req.params.id);
          const taskStatusUpdate = (await req.json()) as UpdateTaskStatusReq;
          const { workerId, taskStatus } = taskStatusUpdate;

          // forbid updating the status if it isn't 'success' or 'error';
          // all others are automatically handled by the server and would otherwise lead to undefined behavior
          if (
            !(
              taskStatus.status === "success" ||
              taskStatus.status === "error" ||
              taskStatus.status === "lost"
            )
          ) {
            return new Response(
              `Task ${id} can't be updated to be ${taskStatus.status}, only 'success', 'error' or 'lost' allowed`,
              { status: 404 },
            );
          }

          // success, error, lost means it's not running anymore
          // so let's remove it from the running state
          const runningTasks = running.get(workerId);
          if (runningTasks) {
            const index = runningTasks.indexOf(id, 0);
            if (index > -1) {
              runningTasks.splice(index, 1);
            }
          }

          // update the status accordingly
          tasksStatus.set(id, taskStatus);

          return new Response("Ok");
        },
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

          // add task to heavy map
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

  // check that no worker got lost by making sure there was
  // atleast one heatbeat every WORKER_TIMEOUT milliseconds
  function workersAreAlive() {
    const now = Date.now();
    for (let [workerId, lastPing] of workersStatus) {
      const diff = now - lastPing;
      if (diff > WORKER_TIMEOUT) {
        var logMsg = `Worker ${workerId} hasn't send a heartbeat within ${WORKER_TIMEOUT}ms (last ping was ${Math.floor(diff / 1000)}s ago)`;
        const workerTasks = running.get(workerId);
        // delete them from running and mark them as lost
        running.delete(workerId);
        workersStatus.delete(workerId);
        if (workerTasks) {
          logMsg += `, it lost tasks: ${workerTasks}`;
          for (const taskId of workerTasks) {
            tasksStatus.set(taskId, { status: "lost", info: null });
          }
        }
        console.log(logMsg);
      } else {
        console.log(`Worker ${workerId} is alive`);
      }
    }
  }
  periodicCallback(workersAreAlive, WORKER_TIMEOUT);
}

// Initialize the server
initializeServer().catch((error: unknown) => {
  console.log(`Failed to start ${SERVER_NAME}: ${String(error)}`);
  process.exit(1);
});
