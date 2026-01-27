from hq.worker import HQWorker, run

if __name__ == "__main__":
    worker = HQWorker(host="http://localhost", port=3000, fetch_n_tasks=3)
    run(worker)
