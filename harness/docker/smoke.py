#!/usr/bin/env python3
import argparse
import json
import time

import redis


def wait_for_server(host: str, port: int, timeout: float = 15.0) -> redis.Redis:
    deadline = time.time() + timeout
    last_error = None
    client = redis.Redis(host=host, port=port, decode_responses=True)
    while time.time() < deadline:
        try:
            if client.ping():
                return client
        except Exception as exc:
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"server did not become ready: {last_error}")


def expect(name: str, got, want, checks: list[dict]) -> None:
    ok = got == want
    checks.append({"name": name, "ok": ok, "got": got, "want": want})
    if not ok:
        raise AssertionError(f"{name}: got {got!r}, want {want!r}")


def initial(client: redis.Redis, prefix: str) -> list[dict]:
    checks: list[dict] = []
    key = f"{prefix}:string"
    hash_key = f"{prefix}:session"
    counter = f"{prefix}:counter"

    expect("ping", client.ping(), True, checks)
    expect("set", client.set(key, "world"), True, checks)
    expect("get", client.get(key), "world", checks)

    expect("hset", client.hset(hash_key, mapping={"user": "ada", "role": "admin"}), 2, checks)
    expect("hget", client.hget(hash_key, "user"), "ada", checks)

    pipe = client.pipeline()
    pipe.incr(counter)
    pipe.expire(counter, 60)
    expect("pipeline", pipe.execute(), [1, True], checks)

    save_result = client.save()
    expect("save", save_result, True, checks)
    return checks


def verify(client: redis.Redis, prefix: str) -> list[dict]:
    checks: list[dict] = []
    expect("ping-after-restart", client.ping(), True, checks)
    expect("get-after-restart", client.get(f"{prefix}:string"), "world", checks)
    expect("hget-after-restart", client.hget(f"{prefix}:session", "role"), "admin", checks)
    expect("counter-after-restart", client.get(f"{prefix}:counter"), "1", checks)
    return checks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--phase", choices=["initial", "verify"], required=True)
    parser.add_argument("--prefix", default="docker-smoke")
    args = parser.parse_args()

    client = wait_for_server(args.host, args.port)
    checks = initial(client, args.prefix) if args.phase == "initial" else verify(client, args.prefix)
    print(json.dumps({"phase": args.phase, "checks": checks}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
