"""Optional histserv integration: remote histogram transport for hq tasks.

Instead of returning whole pickled histograms through the shared-filesystem
result path, tasks stream pre-binned fills to a histserv gRPC server and the
client snapshots the merged result once at the end.

Typical flow (see https://github.com/ijohnkojo/hq_docs/blob/main/docs/architecture/histserv.md):

    # client, once
    remote_hists = init_remote_hists(templates, address="localhost:50051")

    # inside the task / processor
    buffered = make_buffered(remote_hists)
    buffered["4j1b"].fill(observable=..., process=..., variation=..., weight=...)
    flush_buffered(buffered, unique_id=chunk_id)   # one idempotent RPC per hist

    # client, after all tasks finished
    hist_dict = snapshot_hists(remote_hists)
"""

from __future__ import annotations

import typing as tp

try:
    import grpc
    from hist import Hist
    from histserv import Client, RemoteHist
except ImportError as exc:  # pragma: no cover - clear optional-dep message
    raise ImportError(
        "hq.histserv requires histserv and hist. Install them in this "
        "environment (e.g. pip install 'hq[histserv]' or pip install histserv) "
        "before using the histserv transport."
    ) from exc


def init_remote_hists(
    templates: tp.Mapping[str, Hist],
    address: str,
    *,
    token: str | None = None,
) -> dict[str, RemoteHist]:
    """Register histogram templates on a histserv server, once, on the client.

    Returns lightweight ``RemoteHist`` handles (address + hist id + token)
    that are safe to cloudpickle into hq tasks.
    """
    client = Client(address=address)
    return {
        name: client.init(template, token=token)
        for name, template in templates.items()
    }


class BufferedRemoteHist:
    """Buffer ``fill()`` calls locally and send them in one idempotent RPC.

    Presents the same keyword ``fill()`` signature as ``hist.Hist``, so
    existing fill call sites don't change. Nothing touches the network until
    ``flush()`` — a task that raises mid-processing therefore fills nothing,
    and a retried task deduplicates server-side via ``unique_id``.
    """

    def __init__(self, remote: RemoteHist) -> None:
        self._remote = remote
        self._fills: list[dict[str, tp.Any]] = []

    @property
    def remote(self) -> RemoteHist:
        return self._remote

    def fill(self, **kwargs: tp.Any) -> None:
        self._fills.append(kwargs)

    def flush(self, *, unique_id: tp.Any | None = None) -> None:
        """Send all buffered fills as a single ``fill_many`` RPC.

        A duplicate ``unique_id`` (retried/duplicated task) is rejected by the
        server with ``ALREADY_EXISTS``; that means the fills already landed, so
        it is swallowed here to make retries idempotent.
        """
        if not self._fills:
            return
        try:
            self._remote.fill_many(self._fills, unique_id=unique_id)
        except grpc.RpcError as exc:
            already_filled = (
                unique_id is not None
                and exc.code() == grpc.StatusCode.ALREADY_EXISTS
            )
            if not already_filled:
                raise
        self._fills = []


def make_buffered(
    remote_hists: tp.Mapping[str, RemoteHist],
) -> dict[str, BufferedRemoteHist]:
    """Wrap each remote handle in a fill buffer (one per task invocation)."""
    return {name: BufferedRemoteHist(remote) for name, remote in remote_hists.items()}


def flush_buffered(
    buffered: tp.Mapping[str, BufferedRemoteHist],
    *,
    unique_id: tp.Any,
) -> None:
    """Flush every buffered hist, extending ``unique_id`` per hist name.

    ``unique_id`` should identify the unit of work exactly once — for coffea
    chunks, ``(events.metadata["fileuuid"], entrystart, entrystop)``.
    """
    for name, buffered_hist in buffered.items():
        buffered_hist.flush(unique_id=(name, unique_id))


def snapshot_hists(
    remote_hists: tp.Mapping[str, RemoteHist],
    *,
    delete_from_server: bool = False,
) -> dict[str, Hist]:
    """Fetch the merged server-side contents as local ``hist.Hist`` objects."""
    return {
        name: remote.snapshot(delete_from_server=delete_from_server).to_hist()
        for name, remote in remote_hists.items()
    }
