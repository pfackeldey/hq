from hq.worker import HQWorker

if __name__ == "__main__":
    HQWorker.run_loop(host="http://localhost", port=3000, fetch_n=3)
