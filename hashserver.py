#! /usr/bin/env python3
#
# Copyright (C) 2023 Garmin Ltd.
#
# SPDX-License-Identifier: GPL-2.0-only

from datetime import datetime
import argparse
import asyncio
import functools
import json
import logging
import os
import signal
import sys

import websockets
import websockets.server

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import (
    MetaData,
    Column,
    Table,
    Text,
    Integer,
    UniqueConstraint,
    DateTime,
    Index,
    select,
    insert,
    exists,
    literal,
    and_,
    delete,
)
import sqlalchemy.engine
from sqlalchemy.orm import declarative_base

import hashserv

VERSION = "1.0.0"


Base = declarative_base()


class UnihashesV2(Base):
    __tablename__ = "unihashes_v2"
    id = Column(Integer, primary_key=True, autoincrement=True)
    method = Column(Text, nullable=False)
    taskhash = Column(Text, nullable=False)
    unihash = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("method", "taskhash"),
        Index("taskhash_lookup_v3", "method", "taskhash"),
    )


class OuthashesV2(Base):
    __tablename__ = "outhashes_v2"
    id = Column(Integer, primary_key=True, autoincrement=True)
    method = Column(Text, nullable=False)
    taskhash = Column(Text, nullable=False)
    outhash = Column(Text, nullable=False)
    created = Column(DateTime)
    owner = Column(Text)
    PN = Column(Text)
    PV = Column(Text)
    PR = Column(Text)
    task = Column(Text)
    outhash_siginfo = Column(Text)

    __table_args__ = (
        UniqueConstraint("method", "taskhash", "outhash"),
        Index("outhash_lookup_v3", "method", "outhash"),
    )


class ClientLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, client):
        super().__init__(logger)
        self.client = client

    def process(self, msg, kwargs):
        addr = ":".join(str(s) for s in self.client.websocket.remote_address)
        return f"[Client {addr}] {msg}", kwargs


def json_serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

    raise TypeError("Type %s is not serializable" % type(obj))


class Client(object):
    def __init__(
        self,
        websocket,
        logger,
        database,
        read_only,
        upstream,
        backfill_queue,
    ):
        self.websocket = websocket
        self.logger = ClientLoggerAdapter(logger, self)
        self.stats = {}
        self.db = database
        self.upstream = upstream
        self.backfill_queue = backfill_queue
        self.username = getattr(self.websocket, "username", None)

        self.handlers = {
            "get": self.handle_get,
            "get-outhash": self.handle_get_outhash,
            "get-stream": self.handle_get_stream,
            "get-stats": self.handle_get_stats,
        }

        if not read_only:
            self.handlers.update(
                {
                    "report": self.handle_report,
                    "report-equiv": self.handle_equivreport,
                    "reset-stats": self.handle_reset_stats,
                    "backfill-wait": self.handle_backfill_wait,
                    "remove": self.handle_remove,
                }
            )

    async def handle(self):
        try:
            greeting = await self.websocket.recv()
            (proto_name, proto_version) = greeting.split()

            if proto_name != "OEHASHEQUIV":
                self.logger.info("Unknown client protocol %s", proto_name)
                return

            self.proto_name = proto_name

            self.proto_version = tuple(int(s) for s in proto_version.split("."))
            if self.proto_version < (1, 0) or self.proto_version > (1, 1):
                self.logger.info(
                    "Unsupported client protocol version %s",
                    ".".join(str(s) for s in self.proto_version),
                )
                return

            self.logger.info(
                "Connected with %s %s",
                proto_name,
                ".".join(str(s) for s in self.proto_version),
            )

            async for message in self.websocket:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    self.logger.info("JSON decode error: %s", e)
                    return

                self.logger.debug("Request %r", data)

                for k, proc in self.handlers.items():
                    if k in data:
                        if "stream" in k:
                            await proc(data[k])
                            self.logger.debug("%s done", k)
                        else:
                            result = await proc(data[k])
                            self.logger.debug("Response %r", result)
                            await self.websocket.send(
                                json.dumps(result, default=json_serialize)
                            )
                        break
                else:
                    self.logger.info("Unrecognized command: %r" % data)
                    return

        except websockets.exceptions.ConnectionClosedError:
            pass
        self.logger.info("Disconnected")

    async def handle_get_stats(self, request):
        return {"requests": self.stats.copy()}

    async def handle_get_stream(self, request):
        await self.websocket.send(json.dumps("ok"))
        async with self.db.begin():
            async for message in self.websocket:
                if message == "END":
                    await self.websocket.send("ok")
                    break

                (method, taskhash) = message.split()
                d = await self.get_unihash(method, taskhash)
                if d:
                    await self.websocket.send(d["unihash"])
                else:
                    await self.websocket.send("")

    async def handle_get(self, request):
        method = request["method"]
        taskhash = request["taskhash"]
        fetch_all = request.get("all", False)

        async with self.db.begin():
            return await self.get_unihash(method, taskhash, fetch_all)

    async def handle_get_outhash(self, request):
        method = request["method"]
        outhash = request["outhash"]
        taskhash = request["taskhash"]

        async with self.db.begin():
            return await self.get_outhash(method, outhash, taskhash)

    async def handle_report(self, request):
        method = request["method"]
        outhash = request["outhash"]
        taskhash = request["taskhash"]
        outhash_data = {
            "method": method,
            "outhash": outhash,
            "taskhash": taskhash,
            "created": datetime.now(),
        }
        for k in ("owner", "PN", "PV", "PR", "task", "outhash_siginfo"):
            if k in request:
                outhash_data[k] = request[k]

        async with self.db.begin():
            insert_result = await self.insert_outhash(outhash_data)

            if insert_result is not None:
                statement = (
                    select(
                        OuthashesV2.taskhash.label("taskhash"),
                        UnihashesV2.unihash.label("unihash"),
                    )
                    .join(
                        UnihashesV2,
                        and_(
                            UnihashesV2.method == OuthashesV2.method,
                            UnihashesV2.taskhash == OuthashesV2.taskhash,
                        ),
                    )
                    .where(
                        OuthashesV2.method == method,
                        OuthashesV2.outhash == outhash,
                        OuthashesV2.taskhash != taskhash,
                    )
                    .order_by(
                        OuthashesV2.created.asc(),
                    )
                    .limit(1)
                )
                equiv_result = await self.db.execute(statement)
                equiv_row = equiv_result.first()
                self.logger.debug("%s -> %s", statement, equiv_row)
                if equiv_row is not None:
                    # A matching output hash was found. Set our taskhash to the
                    # same unihash since they are equivalent
                    unihash = equiv_row.unihash
                else:
                    # No matching output hash was found. This is probably the
                    # first outhash to be added.
                    unihash = request["unihash"]

                    if self.upstream:
                        upstream_data = await self.upstream.get_outhash(
                            method, outhash, taskhash
                        )
                        if upstream_data is not None:
                            unihash = upstream_data["unihash"]

                await self.insert_unihash(method, taskhash, unihash)

            row = await self.query_equivalent(method, taskhash)
            unihash = row.unihash if row is not None else request["unihash"]

        return {
            "taskhash": request["taskhash"],
            "method": request["method"],
            "unihash": unihash,
        }

    async def handle_equivreport(self, request):
        method = request["method"]
        taskhash = request["taskhash"]
        unihash = request["unihash"]

        async with self.db.begin():
            await self.insert_unihash(method, taskhash, unihash)

        row = await self.query_equivalent(method, taskhash)

        return dict(**row._mapping)

    async def handle_reset_stats(self, request):
        d = {"requests": self.stats.copy()}
        self.stats = {}
        return d

    async def handle_backfill_wait(self, request):
        d = {
            "tasks": self.backfill_queue.qsize(),
        }
        await self.backfill_queue.join()
        return d

    async def handle_remove(self, request):
        condition = request["where"]
        if not isinstance(condition, dict):
            raise TypeError("condition has bad type %s" % type(condition))

        async def do_remove(table):
            nonlocal condition
            where = {}
            for c in table.__table__.columns:
                if c.key in condition and condition[c.key] is not None:
                    where[c] = condition[c.key]

            if where:
                statement = delete(table).where(*[(k == v) for k, v in where.items()])
                self.logger.debug("%s", statement)
                result = await self.db.execute(statement)
                return result.rowcount

            return 0

        count = 0
        async with self.db.begin():
            count += await do_remove(UnihashesV2)
            count += await do_remove(OuthashesV2)

        return {"count": count}

    async def query_equivalent(self, method, taskhash):
        statement = select(
            UnihashesV2.unihash,
            UnihashesV2.method,
            UnihashesV2.taskhash,
        ).where(
            UnihashesV2.method == method,
            UnihashesV2.taskhash == taskhash,
        )
        result = await self.db.execute(statement)
        row = result.first()
        self.logger.debug("%s -> %s", statement, row)
        return row

    async def get_unihash(self, method, taskhash, fetch_all=False):
        if fetch_all:
            statement = (
                select(
                    OuthashesV2,
                    UnihashesV2.unihash.label("unihash"),
                )
                .join(
                    UnihashesV2,
                    and_(
                        UnihashesV2.method == OuthashesV2.method,
                        UnihashesV2.taskhash == OuthashesV2.taskhash,
                    ),
                )
                .where(
                    OuthashesV2.method == method,
                    OuthashesV2.taskhash == taskhash,
                )
                .order_by(
                    OuthashesV2.created.asc(),
                )
                .limit(1)
            )
            result = await self.db.execute(statement)
            row = result.first()
            self.logger.debug("%s -> %s", statement, row)
            if row is not None:
                return dict(**row._mapping)

            if self.upstream:
                d = await self.upstream_client.get_taskhash(method, taskhash, True)
                if d is not None:
                    await self.update_from_upstream(d)
                    return d

        else:
            row = await self.query_equivalent(method, taskhash)
            if row is not None:
                return dict(**row._mapping)

            if self.upstream is not None:
                upstream = await self.upstream.get_unihash(method, taskhash)
                if upstream:
                    await self.backfill_queue.put((method, taskhash))
                    return upstream

        return None

    async def get_outhash(self, method, outhash, taskhash):
        statement = (
            select(OuthashesV2, UnihashesV2.unihash.label("unihash"))
            .join(
                UnihashesV2,
                and_(
                    UnihashesV2.method == OuthashesV2.method,
                    UnihashesV2.taskhash == OuthashesV2.taskhash,
                ),
            )
            .where(
                OuthashesV2.method == method,
                OuthashesV2.outhash == outhash,
            )
            .order_by(
                OuthashesV2.created.asc(),
            )
            .limit(1)
        )
        result = await self.db.execute(statement)
        row = result.first()
        if row is not None:
            return dict(**row._mapping)

        if self.upstream:
            d = await self.upstream.get_outhash(method, outhash, taskhash)
            if d is not None:
                await self.update_from_upastream(d)
                return d

        return None

    async def update_from_upstream(self, data):
        outhash_columns = set(c.key for c in OuthashesV2.__table__.columns)

        await self.insert_unihash(data["method"], data["taskhash"], data["unihash"])
        await self.insert_outhash({k: v for k, v in d.items() if k in outhash_columns})

    async def insert_unihash(self, method, taskhash, unihash):
        data = {
            "method": method,
            "taskhash": taskhash,
            "unihash": unihash,
        }
        keys = sorted(data.keys())

        literals = [literal(data[key]) for key in keys]

        statement = insert(UnihashesV2).from_select(
            keys,
            select(*literals).where(
                ~exists(UnihashesV2.id).where(
                    UnihashesV2.method == method,
                    UnihashesV2.taskhash == taskhash,
                )
            ),
        )
        self.logger.debug("%s", statement)
        await self.db.execute(statement)

    async def insert_outhash(self, outhash_data):
        keys = sorted(outhash_data.keys())
        literals = [literal(outhash_data[key]) for key in keys]

        statement = (
            insert(OuthashesV2)
            .from_select(
                keys,
                select(*literals).where(
                    ~exists(OuthashesV2.id).where(
                        OuthashesV2.method == outhash_data["method"],
                        OuthashesV2.outhash == outhash_data["outhash"],
                        OuthashesV2.taskhash == outhash_data["taskhash"],
                    )
                ),
            )
            .returning(OuthashesV2.id)
        )
        self.logger.debug("%s", statement)
        return await self.db.execute(statement)


async def server_main(
    logger,
    address,
    port,
    engine,
    read_only,
    upstream,
    backfill_queue,
):
    async def handle_client(websocket):
        if upstream:
            logger.info("Connecting to %s as upstream", upstream)
            upstream_client = await hashserv.create_async_client(upstream)
        else:
            upstream_client = None

        async with engine.connect() as client_conn:
            client = Client(
                websocket,
                logger,
                client_conn,
                read_only,
                upstream_client,
                backfill_queue,
            )
            await client.handle()

    stop_server = asyncio.Future()

    def server_exit():
        nonlocal stop_server
        logger.info("Got signal")
        stop_server.set_result(True)

    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM):
        loop.add_signal_handler(s, server_exit)

    logger.info("Listening for clients on %s:%d", address, port)
    async with websockets.server.serve(
        handle_client,
        address,
        port,
    ):
        await stop_server

    logger.debug("Draining backfill queue")
    await backfill_queue.put(None)
    await backfill_queue.join()


async def copy_unihash_from_upstream(client, db, method, taskhash):
    d = await client.get_taskhash(method, taskhash)
    if d is None:
        return

    async with db.begin():
        data = {
            "method": method,
            "taskhash": taskhash,
            "unihash": unihash,
        }
        keys = sorted(data.keys())

        literals = [literal(data[key]) for key in keys]

        statement = insert(UnihashesV2).from_select(
            keys,
            select(*literals).where(
                ~exists(UnihashesV2.id).where(
                    UnihashesV2.method == method,
                    UnihashesV2.taskhash == taskhash,
                )
            ),
        )
        await db.execute(statement)


async def backfill_main(engine, upstream, backfill_queue):
    if upstream is not None:
        client = await hashserv.create_async_client(upstream)
    else:
        client = None

    try:
        async with engine.connect() as db:
            while True:
                item = await backfill_queue.get()
                if item is None:
                    backfill_queue.task_done()
                    break
                method, taskhash = item
                if client is not None:
                    await copy_unihash_from_upstream(client, db, method, taskhash)
                backfill_queue.task_done()
    finally:
        if client is not None:
            client.close()


async def async_main(logger, address, port, database, read_only, upstream):
    backfill_queue = asyncio.Queue()

    logger.info("Using database %s", database)
    engine = create_async_engine(database)

    async with engine.begin() as conn:
        # Create tables
        logger.info("Creating tables...")
        await conn.run_sync(Base.metadata.create_all)

    await asyncio.gather(
        server_main(
            logger,
            address,
            port,
            engine,
            read_only,
            upstream,
            backfill_queue,
        ),
        backfill_main(engine, upstream, backfill_queue),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Hash Equivalence Websocket Server. Version=%s" % VERSION,
    )

    parser.add_argument(
        "-b",
        "--bind",
        default=os.environ.get("HASHSERVER_BIND", "localhost"),
        help='Bind address (HASHSERVER_BIND, default "%(default)s")',
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=os.environ.get("HASHSERVER_PORT", 9000),
        help="Bind port (HASHSERVER_PORT, default %(default)s)",
    )
    parser.add_argument(
        "-d",
        "--database",
        default=os.environ.get("HASHSERVER_DB", "sqlite+aiosqlite:///hashserv.db"),
        help='Database file (HASHSERVER_DB, default "%(default)s")',
    )
    parser.add_argument(
        "--db-username",
        default=os.environ.get("HASHSERVER_DB_USERNAME", None),
        help="Database username (HASHSERVER_DB_USERNAME)",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("HASHSERVER_DB_PASSWORD", None),
        help="Database password (HASHSERVER_DB_PASSWORD)",
    )
    parser.add_argument(
        "-l",
        "--log",
        default=os.environ.get("HASHSERVER_LOG_LEVEL", "WARNING"),
        help='Set logging level (HASHSERVER_LOG_LEVEL, default "%(default)s")',
    )
    parser.add_argument(
        "-u",
        "--upstream",
        default=os.environ.get("HASHSERVER_UPSTREAM", None),
        help="Upstream hashserv to pull hashes from (HASHSERVER_UPSTREAM)",
    )
    parser.add_argument(
        "-r",
        "--read-only",
        action="store_true",
        help="Disallow write operations from clients (HASHSERVER_READONLY)",
    )

    args = parser.parse_args()

    read_only = args.read_only or (os.environ.get("HASHSERVER_READONLY") == "1")

    level = getattr(logging, args.log.upper(), None)
    if not isinstance(level, int):
        raise ValueError("Invalid log level: %s" % args.log)

    logger = logging.getLogger("hashserv-ws")
    logger.setLevel(level)
    console = logging.StreamHandler()
    console.setLevel(level)
    logger.addHandler(console)

    url = sqlalchemy.engine.make_url(args.database)

    if args.db_username is not None:
        url = url.set(username=args.db_username)

    if args.db_password is not None:
        url = url.set(password=args.db_password)

    asyncio.run(async_main(logger, args.bind, args.port, url, read_only, args.upstream))


if __name__ == "__main__":
    try:
        ret = main()
    except Exception:
        ret = 1
        import traceback

        traceback.print_exc()
    sys.exit(ret)
