import time
from hq.client import HQClient


def my_function():
    time.sleep(1)
    print("Hello, World!")


def my_map_fun(i: int):
    time.sleep(1)
    return i * 2


if __name__ == "__main__":
    with HQClient(host="http://localhost", port=3000) as client:
        task_id = client.submit(my_function)
        print(f"[submit] Task ID: {task_id}")

        task_ids = client.map(my_map_fun, range(10))
        print(f"[map] Task IDs: {task_ids}")
