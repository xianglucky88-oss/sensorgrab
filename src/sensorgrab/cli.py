import json
import sys
from . import pipeline
from .config import GATEWAY_FIXTURE


def main():
    result = {"legacy": pipeline.run()}
    if GATEWAY_FIXTURE.exists():
        result["gateway"] = pipeline.run(fixture=GATEWAY_FIXTURE)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
