"""
Sidecar 2.0 -- Phase 0 spike, part 2: CDP session-attach for iframe drilling.

nodriver's core Connection/Transaction/_listener has NO support for the
flattened Target.attachToTarget session-multiplexing mode -- confirmed by
reading its actual installed source (not guessed):
  - Transaction.message never includes a "sessionId" field.
  - Connection._listener demuxes incoming messages purely by "id" (command
    responses) or event TYPE (events) -- with zero sessionId awareness, so
    events from an attached child session would either collide with the
    parent's own handlers or go nowhere.
  - There's a chunk of commented-out code in nodriver's own tab.py (a
    "TargetTransaction"/session wrapper) that was clearly a prior, abandoned
    attempt at exactly this.

This module finishes that: it lets you attach to a nested target (e.g. a
cross-origin iframe -- an OOPIF, which gets its own CDP Target but whose
direct per-target websocket nodriver tries to open 404s, since Chrome only
serves the top-level /devtools/page/{id} endpoint that way) and issue
session-scoped commands + receive session-scoped events, multiplexed over
the PARENT tab's already-open websocket, which is how flatten=True mode
is actually meant to be used.

This is a monkeypatch of one parent Connection instance's `_listener`
coroutine (not the whole nodriver library) -- copied near-verbatim from
`connection.py`'s real `_listener`, with exactly one change: route events
whose raw message carries a registered `sessionId` to that session's own
handler table instead of the parent's. Command/response correlation needs
NO patching at all -- reusing the parent's own id counter + `mapper` dict
for session-scoped commands means responses already route correctly through
the existing, unmodified code path.
"""

import asyncio
import json
import types
from asyncio import iscoroutine, iscoroutinefunction

import websockets.exceptions

import nodriver as uc
from nodriver.core.connection import ProtocolException, Transaction


class SessionTransaction(Transaction):
    """Identical to nodriver's own Transaction, except .message carries the
    sessionId flattened-mode CDP requires on every outgoing command."""

    def __init__(self, cdp_obj, session_id):
        super().__init__(cdp_obj)
        self.session_id = session_id

    @property
    def message(self):
        return json.dumps({
            "sessionId": str(self.session_id),
            "method": self.method,
            "params": self.params,
            "id": self.id,
        })


class AttachedSession:
    """A CDP session attached to a nested target, multiplexed over `parent`'s
    existing websocket. Deliberately mirrors Connection's own .send()/
    .add_handler() surface so capture code doesn't need to care whether it's
    holding a real Tab or one of these."""

    def __init__(self, parent, session_id, target_id):
        self.parent = parent
        self.session_id = session_id
        self.target_id = target_id
        self.handlers = {}

    async def send(self, cdp_obj):
        tx = SessionTransaction(cdp_obj, self.session_id)
        tx.id = next(self.parent.__count__)
        self.parent.mapper[tx.id] = tx
        await self.parent.websocket.send(tx.message)
        return await tx

    def add_handler(self, event_type, callback):
        self.handlers.setdefault(event_type, []).append(callback)


async def _patched_listener(self):
    """Copy of Connection._listener (connection.py) with one change: events
    carrying a sessionId that matches something in self._session_registry
    are dispatched to THAT session's handlers, not the parent's."""
    while True:
        try:
            async with self._lock:
                raw = await asyncio.wait_for(self.websocket.recv(), 0.05)
        except ProtocolException:
            break
        except websockets.exceptions.ConnectionClosedOK:
            await self.disconnect()
            break
        except websockets.exceptions.ConnectionClosed:
            await self.disconnect()
            break
        except asyncio.TimeoutError:
            await asyncio.sleep(0.05)
            continue
        except Exception:
            raise
        else:
            message = json.loads(raw)
            if "id" in message:
                tx = self.mapper.pop(message["id"])
                tx(**message)
                continue

            try:
                event = uc.cdp.util.parse_json_event(message)
            except Exception:
                continue

            sid = message.get("sessionId")
            registry = getattr(self, "_session_registry", {})
            handlers = registry[sid].handlers if (sid and sid in registry) else self.handlers

            if type(event) not in handlers:
                continue
            callbacks = handlers[type(event)]
            if not callbacks:
                continue
            for callback in callbacks:
                try:
                    if iscoroutinefunction(callback) or iscoroutine(callback):
                        try:
                            asyncio.create_task(callback(event, self))
                        except TypeError:
                            asyncio.create_task(callback(event))
                    else:
                        try:
                            callback(event, self)
                        except TypeError:
                            callback(event)
                except Exception as e:
                    print(f"    [session_attach] handler callback error: {e}")


def _ensure_patched(parent):
    if getattr(parent, "_session_registry", None) is not None:
        return
    parent._session_registry = {}
    if parent._listener_task:
        parent._listener_task.cancel()
    parent._listener = types.MethodType(_patched_listener, parent)
    parent._listener_task = asyncio.ensure_future(parent._listener())


async def attach_to_target(parent_tab, target_id) -> AttachedSession:
    """parent_tab: any already-connected Tab/Connection (e.g. browser.main_tab).
    target_id: the iframe target's target_id (from a TargetInfo in
    browser.targets, e.g. iframe_conn.target.target_id)."""
    session_id = await parent_tab.send(uc.cdp.target.attach_to_target(target_id, flatten=True))
    _ensure_patched(parent_tab)
    session = AttachedSession(parent_tab, session_id, target_id)
    parent_tab._session_registry[str(session_id)] = session
    return session
