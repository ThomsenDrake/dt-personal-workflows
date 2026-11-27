"""Worker entrypoint: discovers every workflow in worker.workflows and runs the worker.

Run from the repo root with the Mistral CLI ("mistral dev"), or directly:

    cd workflows/epub-voxtral-audiobook/worker && uv sync
    PYTHONPATH=src uv run python -m worker.entrypoints.worker
"""
import asyncio

import mistralai.workflows as workflows


async def main() -> None:
    workflow_classes = workflows.discover_all_workflows_in_package("worker.workflows")
    run_worker = getattr(workflows, "run_worker", None)
    if callable(run_worker):
        # Current SDKs expose run_worker at module level.
        await run_worker(workflow_classes)
        return
    # Fallback for SDK versions where run_worker hangs off the client.
    from mistralai.workflows.client import get_mistral_client

    client = get_mistral_client()
    await client.workflows.run_worker(workflow_classes)


if __name__ == "__main__":
    asyncio.run(main())
