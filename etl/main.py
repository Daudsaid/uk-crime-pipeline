import time
from extract import extract
from transform import transform
from load import load


def _phase(name: str):
    """Context manager that prints timing for a named phase."""
    class _Timer:
        def __enter__(self):
            print(f"\n[{name}] starting...")
            self.start = time.perf_counter()
            return self

        def __exit__(self, *_):
            elapsed = time.perf_counter() - self.start
            print(f"[{name}] done in {elapsed:.2f}s")

    return _Timer()


def run():
    pipeline_start = time.perf_counter()

    with _phase("extract"):
        raw = extract()

    with _phase("transform"):
        df = transform(raw)

    with _phase("load"):
        load(df)

    total = time.perf_counter() - pipeline_start
    print(f"\nPipeline complete — {len(df)} records in {total:.2f}s")


if __name__ == "__main__":
    run()
