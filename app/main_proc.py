import multiprocessing
import uvicorn


def run_worker():
    from app.worker import main
    main()


def run_api():
    uvicorn.run("app.main:app", host="0.0.0.0", port=int(__import__("os").environ.get("PORT", "8000")))


if __name__ == "__main__":
    worker_proc = multiprocessing.Process(target=run_worker, daemon=True)
    worker_proc.start()
    run_api()
