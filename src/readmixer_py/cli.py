"""Installed command line; stderr diagnostics never render raw input values."""

import argparse
from contextlib import contextmanager
from dataclasses import asdict, fields, is_dataclass
import json
import re
from pathlib import Path
import signal
import sys
import threading

from . import __version__, inspect, mix, recover, verify
from .config import (
    InspectConfig, MixConfig, RecoverConfig, RecoveryMode, StorageMode, VerifyConfig,
    parse_size,
)
from .errors import CancelledError, ConfigurationError, ReadMixerError
from ._cli_cancellation import defer_cli_signal


class Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default embeds unknown arguments, paths and selector values.
        self.print_usage(sys.stderr)
        self.exit(2, "readmixer-py: invalid arguments; see --help. Example: "
                  "readmixer-py inspect --samples inputs/samples.tsv --deep\n")


def _size(value):
    try:
        return parse_size(value)
    except ConfigurationError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _positive(value):
    if not value.isascii() or not value.isdigit() or len(value) > 20:
        raise argparse.ArgumentTypeError("expected a positive integer")
    result = int(value)
    if not 0 < result < 1 << 64:
        raise argparse.ArgumentTypeError("positive integer outside supported bounds")
    return result


def build_parser():
    parser = Parser(prog="readmixer-py", allow_abbrev=False,
                    description="Paired FASTQ mixing, complete verification, and sample recovery. Development build.")
    parser.add_argument("--version", action="version", version=f"readmixer-py {__version__}")
    commands = parser.add_subparsers(dest="operation", required=True)
    for name in ("inspect", "mix", "verify", "recover"):
        sub = commands.add_parser(name, allow_abbrev=False)
        if name in ("inspect", "mix"):
            source = sub.add_mutually_exclusive_group(required=True)
            source.add_argument("--input", type=Path, dest="input_dir", metavar="DIR")
            source.add_argument("--samples", type=Path, metavar="TSV")
        else:
            sub.add_argument("--manifest", type=Path, required=True)
        if name == "inspect":
            sub.add_argument("--deep", action="store_true", help="validate both streams and count normalized bytes")
        if name == "verify":
            sub.add_argument("--originals", type=Path, metavar="TSV")
        if name == "recover":
            sub.add_argument("--sample", action="append", required=True, metavar="ID")
            sub.add_argument("--io", choices=[m.value for m in RecoveryMode], default="indexed")
        if name in ("mix", "recover"):
            sub.add_argument("--output", type=Path, required=True, metavar="DIR")
            sub.add_argument("--max-artifact-bytes", type=_size,
                             default=5_000_000_000 if name == "mix" else None,
                             help="physical bytes per final file (65564B..5000000000B)")
            sub.add_argument("--compression-level", type=int, choices=range(10), default=6)
        sub.add_argument("--workers", type=_positive, help="upper bound on CPU concurrency; inspection executes serially")
        sub.add_argument("--max-ram", type=_size, metavar="SIZE", help="aggregate process allowance in explicit byte units")
        if name != "inspect":
            sub.add_argument("--storage", choices=[m.value for m in StorageMode], default="auto")
            sub.add_argument("--temp-dir", type=Path, metavar="DIR")
        sub.add_argument("--max-record-bytes", type=_size, metavar="SIZE",
                         default=1 << 20 if name in ("inspect", "mix") else None,
                         help="maximum normalized original mate record (default 1MiB for inputs)")
        sub.add_argument("--max-total-uncompressed", type=_size, metavar="SIZE")
        sub.add_argument("--log-level", choices=("error", "warning", "info", "debug"), default="info")
        sub.add_argument("--json", action="store_true", help="bounded redacted JSON report")
    return parser


@contextmanager
def _signals():
    """CLI-only signal handling; library imports leave process state unchanged."""
    previous = {}

    def stop(signum, frame):
        error = CancelledError(signal=signum)
        if not defer_cli_signal(error, frame):
            raise error

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, stop)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def _committed_note(error, result=None):
    for note in getattr(error, "__notes__", ()):
        if re.fullmatch(r"outputs are durably committed; completion marker: [A-Za-z0-9][A-Za-z0-9._-]{0,254}", note):
            print(f"readmixer-py: {note}", file=sys.stderr)
            return
    if result is not None and getattr(result, "completed", False):
        marker = getattr(result, "manifest", None) or getattr(result, "receipt", None)
        if isinstance(marker, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}", marker):
            print(f"readmixer-py: outputs are durably committed; completion marker: {marker}", file=sys.stderr)


def main(argv=None) -> int:
    args = vars(build_parser().parse_args(argv))
    operation = args.pop("operation")
    json_output = args.pop("json")
    args.pop("log_level")
    if "storage" in args:
        args["storage"] = StorageMode(args["storage"])
    if operation == "recover":
        args["sample"] = tuple(args["sample"])
        args["io"] = RecoveryMode(args["io"])
    configs = {"inspect": InspectConfig, "mix": MixConfig, "verify": VerifyConfig, "recover": RecoverConfig}
    operations = {"inspect": inspect, "mix": mix, "verify": verify, "recover": recover}
    result = None
    try:
        with _signals():
            result = operations[operation](configs[operation](**args))
            if json_output:
                # Exclude inventories before conversion, so JSON rendering does not
                # copy a potentially large private report graph only to discard it.
                public = {item.name: (asdict(value) if is_dataclass(value) else value)
                          for item in fields(result) if item.name != "outputs"
                          for value in (getattr(result, item.name),)}
                print(json.dumps(public, allow_nan=False, separators=(",", ":")))
            elif operation == "inspect":
                detail = "decoded counts unknown (use --deep)" if result.pairs is None else (
                    f"{result.pairs} pairs; {result.normalized_bytes} normalized bytes; "
                    f"mix_eligible={str(result.mix_eligible).lower()}"
                )
                print(f"Inspection completed: {result.samples} samples; {detail}.")
            else:
                detail = f"{result.samples} samples; {result.pairs} pairs"
                if operation == "mix":
                    detail += f"; manifest: {result.manifest}"
                elif operation == "recover":
                    detail += f"; scope: {result.validation_scope}; receipt: {result.receipt}"
                else:
                    detail += f"; scope: {result.validation_scope}"
                print(f"{operation.capitalize()} completed: {detail}.")
                for warning in getattr(result, "warnings", ()):
                    print(f"readmixer-py: cleanup warning: {warning}", file=sys.stderr)
            return 0
    except ConfigurationError as exc:
        print(f"readmixer-py: configuration: {exc}", file=sys.stderr)
        return 2
    except CancelledError as exc:
        print("readmixer-py: operation cancelled", file=sys.stderr)
        _committed_note(exc, result)
        return 143 if exc.signal == signal.SIGTERM else 130
    except KeyboardInterrupt as exc:
        print("readmixer-py: operation cancelled", file=sys.stderr)
        _committed_note(exc, result)
        return 130
    except ReadMixerError as exc:
        print(f"readmixer-py: {type(exc).__name__}: {exc}", file=sys.stderr)
        _committed_note(exc, result)
        return 1
    except (OSError, MemoryError) as exc:
        print("readmixer-py: system I/O or allocation failure", file=sys.stderr)
        _committed_note(exc, result)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
