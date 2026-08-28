import sys
import time

from hq.client import HQClient
from hq.util import generate_queue_name

HOST = "https://localhost"
PORT = 3000
VERIFY = "cert.pem"

N_MAP_TASKS = 10  
TASK_SLEEP_S = 0.5

def my_function() -> str:
    time.sleep(0.5)
    return "Hello, World!"


def my_map_fun(i: int) -> int:
    time.sleep(TASK_SLEEP_S)
    return i * 2


def my_faulty_fun() -> None:
    raise ValueError("This is a faulty function")


if __name__ == "__main__":
    # Pick a unique queue name so multiple users can share one HQ server.
    queue = sys.argv[1] if len(sys.argv) > 1 else generate_queue_name()
    host = sys.argv[2] if len(sys.argv) > 2 else HOST
    port = int(sys.argv[3]) if len(sys.argv) > 3 else PORT
    verify = sys.argv[4] if len(sys.argv) > 4 else VERIFY

    print(f"queue={queue}")
    print(
        f"start worker: uv run example/simple/worker.py "
        f"{queue} {host} {port} {verify}"
    )

    with HQClient(host=host, port=port, queue=queue, verify=verify) as client:
        task_id = client.submit(my_function)
        print(f"[submit] Task ID: {task_id}")

        task_ids = client.map(my_map_fun, range(N_MAP_TASKS))
        print(f"[map] Task IDs: {len(task_ids)} tasks")

        faulty_task_id = client.submit(my_faulty_fun)
        print(f"[submit] Faulty Task ID: {faulty_task_id}")

        while True:
            time.sleep(3)
            print("\nChecking tasks status:")
            all_ids = [task_id, *task_ids, faulty_task_id]
            checked_many = client.check(*all_ids)
            statuses = []
            for _id, checked in zip(all_ids, checked_many):
                status = checked["status"] if checked is not None else "missing"
                statuses.append(status)
                print(f"[status] Task ID: {_id}, Status: {checked}")

            if all(status in {"success", "error", "lost"} for status in statuses):
                break
