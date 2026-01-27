import time
from hq.client import HQClient


def my_function() -> str:
    time.sleep(1)
    return "Hello, World!"


def my_map_fun(i: int) -> int:
    time.sleep(1)
    return i * 2


def my_faulty_fun() -> None:
    raise ValueError("This is a faulty function")


if __name__ == "__main__":
    with HQClient(host="http://localhost", port=3000) as client:
        # submit some tasks
        task_id = client.submit(my_function)
        print(f"[submit] Task ID: {task_id}")

        task_ids = client.map(my_map_fun, range(10))
        print(f"[map] Task IDs: {task_ids}")

        faulty_task_id = client.submit(my_faulty_fun)
        print(f"[submit] Faulty Task ID: {faulty_task_id}")

        # check their status
        while True:
            time.sleep(3)
            print("\nChecking tasks status:")
            statuses = []
            for _id in [task_id, *task_ids, faulty_task_id]:
                checked = client.check(_id)
                statuses.append(checked["status"])
                print(f"[status] Task ID: {_id}, Status: {checked}")

            # break if all of them have been finished
            if all(status in {"success", "error", "lost"} for status in statuses):
                break
