from contextlib import asynccontextmanager

import async_module
import fastapi
import hamilton_sdk.adapters

from hamilton import base
from hamilton.experimental import h_async

# can instantiate a driver once for the life of the app:
dr = None


@asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    global dr

    tracker_async = await hamilton_sdk.adapters.AsyncHamiltonTracker(
        project_id=4,
        username="elijah",
        dag_name="async_tracker",
    ).ainit()
    # tracker_sync = hamilton_sdk.adapters.HamiltonTracker(
    #     project_id=4,
    #     username="elijah",
    #     dag_name="sync_tracker",
    # )
    dr = await h_async.AsyncDriver(
        {},
        async_module,
        result_builder=base.DictResult(),
        adapters=[tracker_async],
    ).ainit()
    yield


app = fastapi.FastAPI(lifespan=lifespan)


@app.post("/execute")
async def call(request: fastapi.Request) -> dict:
    """Handler for pipeline call"""
    input_data = {"request": request}
    # Can instantiate a driver within a request as well:
    # dr = h_async.AsyncDriver({}, async_module, result_builder=base.DictResult())
    result = await dr.execute(["pipeline"], inputs=input_data)
    # dr.visualize_execution(["pipeline"], "./pipeline.dot", {"format": "png"}, inputs=input_data)
    return result


if __name__ == "__main__":
    # If you run this as a script, then the app will be started on localhost:8000
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
