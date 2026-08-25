"""
Watches a free, unofficial pump.fun websocket relay for new token launches.

IMPORTANT: pump.fun has no official public API. This connects to a third-party
relay (configurable via PUMPFUN_WS_URL) that mirrors on-chain launch events.
These relays are run by independent devs, not pump.fun - they can change their
message format or go offline without notice. If this stops producing events,
print the raw payload (see below) to check whether the schema changed, or
swap PUMPFUN_WS_URL for an alternative relay.
"""
import asyncio
import json
import logging

import websockets

from models import Candidate

log = logging.getLogger("pumpfun")


async def watch_pumpfun(ws_url: str, queue: asyncio.Queue, min_initial_buy_sol: float):
    """
    Connects to the relay, subscribes to new-token events, and pushes a
    Candidate onto `queue` for every launch that clears the minimum
    initial-buy filter. Reconnects automatically on drops.
    """
    backoff = 1
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                log.info("Connected to pump.fun relay at %s", ws_url)
                backoff = 1
                await ws.send(json.dumps({"method": "subscribeNewToken"}))

                async for raw in ws:
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    candidate = _parse_new_token_event(event, min_initial_buy_sol)
                    if candidate:
                        await queue.put(candidate)

        except (websockets.ConnectionClosed, OSError) as e:
            log.warning("pump.fun websocket dropped (%s). Reconnecting in %ss...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def _parse_new_token_event(event: dict, min_initial_buy_sol: float):
    """
    Field names come from the relay's convention (mint/name/symbol/
    marketCapSol/solAmount for the creator's initial buy). If your relay
    uses different field names, this is the only place you need to edit -
    uncomment the log.debug line below to inspect raw payloads.
    """
    # log.debug("raw pumpfun event: %s", event)

    tx_type = event.get("txType") or event.get("type")
    if tx_type not in (None, "create"):
        return None  # not a new-launch event (e.g. a trade update)

    mint = event.get("mint") or event.get("mintAddress")
    if not mint:
        return None

    initial_buy_sol = float(event.get("solAmount") or event.get("initialBuy") or 0.0)
    if initial_buy_sol < min_initial_buy_sol:
        return None

    return Candidate(
        address=mint,
        symbol=event.get("symbol", "?"),
        name=event.get("name", "?"),
        first_seen_source="pumpfun",
        initial_buy_sol=initial_buy_sol,
        market_cap_sol=float(event.get("marketCapSol") or 0.0),
    )
