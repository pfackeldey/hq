import time
from collections import Counter

from hq.executor import HQExecutor

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
    with HQExecutor(host=HOST, port=PORT, n_workers=2, verify=VERIFY) as ex:
        print(f"queue={ex.queue}")

        task_id = ex.submit(my_function)
        task_ids = ex.map(my_map_fun, range(N_MAP_TASKS))
        faulty_id = ex.submit(my_faulty_fun)

        ok_ids = [task_id, *task_ids]
        all_ids = [*ok_ids, faulty_id]

        statuses = ex.wait(*all_ids, poll_interval=1.0)
        counts = Counter(
            (s["status"] if s is not None else "missing") for s in statuses
        )
        print(f"done: {dict(counts)}  ({len(all_ids)} tasks)")

        results = ex.gather(*ok_ids)
        print("results:", results)

        try:
            ex.gather(faulty_id)
        except RuntimeError as e:
            print(f"faulty task raised as expected: {e}")