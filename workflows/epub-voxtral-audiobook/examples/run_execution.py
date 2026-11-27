"""Start an execution of the epub-voxtral-audiobook workflow.

Usage:
    export MISTRAL_API_KEY=...
    python workflows/epub-voxtral-audiobook/examples/run_execution.py             # examples/sample_input.json
    python workflows/epub-voxtral-audiobook/examples/run_execution.py my_input.json

Note: the workflow is on_behalf_of=True, so the API key's caller must carry a
full user identity (user_id / organization_id) — executions from keys without
one are rejected. Starting it from Vibe/Studio as a signed-in user is the
intended path (see skill/SKILL.md).
"""
import json
import os
import sys
from pathlib import Path

from mistralai.client import Mistral


def main() -> None:
    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
    default_path = Path(__file__).parent / "sample_input.json"
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path
    with path.open() as f:
        payload = json.load(f)
    print("Triggering epub-voxtral-audiobook with input: " + str(path))
    execution = client.workflows.execute_workflow(
        workflow_identifier="epub-voxtral-audiobook",
        deployment_name="epub-audiobook-worker",
        input=payload,
        wait_for_result=True,
    )
    raw = execution.model_dump_json(indent=2)
    print(raw)
    try:
        data = json.loads(raw)
        output = data.get("output") if isinstance(data, dict) else None
        if isinstance(output, dict) and output.get("files"):
            print(chr(10) + "===== OUTPUT FILES =====" + chr(10))
            for f in output["files"]:
                print(f"- {f.get('name')} ({f.get('mime_type')}, {f.get('size_bytes')} bytes): {f.get('view_url')}")
            print(chr(10) + "===== CAST =====" + chr(10))
            for c in output.get("cast", []):
                print(f"- {c['name']}: {c['voice_id']} ({c['voice_name']})")
    except Exception:
        pass


if __name__ == "__main__":
    main()
