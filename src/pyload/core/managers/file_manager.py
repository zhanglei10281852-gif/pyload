import time
from itertools import chain
from threading import RLock

from ..datatypes.enums import Destination
from ..datatypes.exceptions import RemovalError
from ..datatypes.pyfile import ABORT_TIMEOUT
from ..utils.struct.lock import lock
from .event_manager import InsertEvent, RemoveEvent, UpdateEvent


def change(func):
    def new(self, *args):
        self.unchanged = False
        self.filecount = -1
        self.queuecount = -1
        self.job_cache = {}
        return func(self, *args)

    return new


class FileManager:
    """
    Handles all request made to obtain information, modify status or other request for
    links or packages.
    """

    def __init__(self, core):
        """
        Constructor.
        """
        self.pyload = core
        self._ = core._

        # translations
        self.status_msg = [
            self._("finished"),
            self._("offline"),
            self._("online"),
            self._("queued"),
            self._("skipped"),
            self._("waiting"),
            self._("temp. offline"),
            self._("starting"),
            self._("failed"),
            self._("aborted"),
            self._("decrypting"),
            self._("custom"),
            self._("downloading"),
            self._("processing"),
            self._("unknown"),
        ]

        # TODO: purge the cache
        self.cache = {}  #: holds instances for files
        self.package_cache = {}  #: same for packages

        self.job_cache = {}

        self.lock = RLock()  # TODO: should be a Lock w/o R
        # self.lock._Verbose__verbose = True

        self.filecount = -1  #: if an invalid value is set get current value from db
        self.queuecount = -1  #: number of package to be loaded
        self.unchanged = False  #: determines if any changes was made since last call

    # ----------------------------------------------------------------------
    def save(self):
        """
        saves all data to backend.
        """
        self.pyload.db.commit()

    # ----------------------------------------------------------------------
    def sync_save(self):
        """
        saves all data to backend and waits until all the data is written.
        """
        pyfiles = self.cache.values()
        for pyfile in pyfiles:
            pyfile.sync()

        pypacks = self.package_cache.values()
        for pypack in pypacks:
            pypack.sync()

        self.pyload.db.sync_save()

    @lock
    def get_complete_data(self, queue=Destination.QUEUE):
        """
        gets a complete data representation.
        """
        queue = queue.value
        data = self.pyload.db.get_all_links(queue)
        packs = self.pyload.db.get_all_packages(queue)

        data.update((x.id, x.to_db_dict()[x.id]) for x in self.cache.values())

        for x in self.package_cache.values():
            if x.queue != queue or x.id not in packs:
                continue
            packs[x.id].update(x.to_dict()[x.id])

        for key, value in data.items():
            if value["package"] in packs:
                packs[value["package"]]["links"][key] = value

        return packs

    @lock
    def get_info_data(self, queue=Destination.QUEUE):
        """
        gets a data representation without links.
        """
        queue = queue.value
        packs = self.pyload.db.get_all_packages(queue)
        for x in self.package_cache.values():
            if x.queue != queue or x.id not in packs:
                continue
            packs[x.id].update(x.to_dict()[x.id])

        return packs

    @lock
    @change
    def add_links(self, urls, package):
        """
        adds links.
        """
        self.pyload.addon_manager.dispatch_event("links_added", urls, package)

        data = self.pyload.plugin_manager.parse_urls(urls)

        self.pyload.db.add_links(data, package)
        self.pyload.thread_manager.create_info_thread(data, package)

        p = self.get_package(package)
        self.pyload.event_manager.add_event(
            UpdateEvent("pack", package, "collector" if not p.queue else "queue")
        )

    # ----------------------------------------------------------------------
    @lock
    @change
    def add_package(self, name, folder, queue=Destination.QUEUE):
        """
        adds a package, default to link collector.
        """
        last_id = self.pyload.db.add_package(name, folder, queue.value)
        p = self.pyload.db.get_package(last_id)
        e = InsertEvent(
            "pack",
            last_id,
            p.order,
            "collector" if queue is Destination.COLLECTOR else "queue",
        )
        self.pyload.event_manager.add_event(e)
        return last_id

    # ----------------------------------------------------------------------
    @change
    def delete_package(self, id):
        """
        delete package and all contained links.

        Lifecycle: claim every running child, wait (bounded) for all of their
        transfers to converge, delete package and links from the database in a
        single transaction, and only then publish the removal once. If any
        child cannot be stopped or the database write fails, every entry is
        restored to a visible, stopped and retryable state and
        :class:`RemovalError` is raised.

        :return: True on success, False if the package did not exist
        """
        with self.lock:
            p = self.get_package(id)
            if not p:
                # repeat deletion: nothing to remove, no event or callback
                self.release_package(id)
                return False

            oldorder = p.order
            queue = p.queue

            # all live (possibly assigned) instances of this package's files
            children = [
                pyfile
                for pyfile in list(self.cache.values())
                if pyfile.packageid == id
            ]

            # claim before signalling, so worker epilogues cannot write records
            # back or fire late callbacks while the package is being removed
            for pyfile in children:
                pyfile.claim_removal()

        # wait for every transfer to converge WITHOUT holding the manager lock:
        # worker epilogues (package stats, addon callbacks) may legitimately
        # take this lock while winding down
        deadline = time.monotonic() + ABORT_TIMEOUT
        unconverged = []
        for pyfile in children:
            remaining = deadline - time.monotonic()
            if not pyfile.abort_download(timeout=max(remaining, 0)):
                unconverged.append(pyfile)

        if unconverged:
            reason = self._("Transfer did not respond to cancellation in time")
            failed_ids = [pyfile.id for pyfile in unconverged]
            self.pyload.log.warning(
                self._(
                    "Deleting package {id} failed, transfer(s) {ids} did not stop"
                ).format(id=id, ids=failed_ids)
            )
            with self.lock:
                self._restore_children(children, failed_ids, reason)
            raise RemovalError(reason, file_ids=failed_ids, package_ids=[id])

        # every child is now unassigned with its request resources closed
        with self.lock:
            for pyfile in children:
                self.release_link(pyfile.id)

            try:
                self.pyload.db.delete_package(p)
                self.pyload.db.sync_save()
                if self.pyload.db.get_package(id) is not None:
                    raise RuntimeError("package still present after deletion")
            except Exception as exc:
                try:
                    package_gone = self.pyload.db.get_package(id) is None
                except Exception:
                    package_gone = False

                if package_gone:
                    self.pyload.log.warning(
                        self._(
                            "Package {id} was deleted but the operation reported: {msg}"
                        ).format(id=id, msg=exc)
                    )
                else:
                    reason = self._("Database error while deleting package")
                    self.pyload.log.error(
                        self._("Deleting package {id} failed: {msg}").format(
                            id=id, msg=exc
                        )
                    )
                    self._restore_children(
                        children, [pyfile.id for pyfile in children], reason
                    )
                    raise RemovalError(reason, package_ids=[id]) from exc

            # removal is durable: publish exactly once
            e = RemoveEvent("pack", id, "collector" if not queue else "queue")
            self.pyload.event_manager.add_event(e)
            self.pyload.addon_manager.dispatch_event("package_deleted", id)

            if id in self.package_cache:
                del self.package_cache[id]

            packs = self.package_cache.values()
            for pack in packs:
                if pack.queue == queue and pack.order > oldorder:
                    pack.order -= 1
                    pack.notify_change()

        return True

    # ----------------------------------------------------------------------
    @change
    def delete_link(self, id):
        """
        delete a link.

        Lifecycle: claim the file, wait (bounded) for its transfer to
        converge, delete the record from the database, and only then publish
        the removal once. If the transfer cannot be stopped or the database
        write fails, the entry stays visible in a stopped, retryable state and
        :class:`RemovalError` is raised.

        :return: True on success, False if the link did not exist
        """
        with self.lock:
            f = self.get_file(id)
            if not f:
                # repeat deletion: nothing to remove, no event or callback
                self.release_link(id)
                return False

            pid = f.packageid
            oldorder = f.order

            package = self.get_package(pid)
            destination = (
                "collector" if not package or not package.queue else "queue"
            )

            claimed = f.claim_removal()

        # wait for the transfer to converge WITHOUT holding the manager lock,
        # the worker epilogue may legitimately need it while winding down
        converged = f.abort_download(timeout=ABORT_TIMEOUT)

        if not converged:
            reason = self._("Transfer did not respond to cancellation in time")
            self.pyload.log.warning(
                self._("Deleting link {id} failed, transfer did not stop").format(
                    id=id
                )
            )
            with self.lock:
                self._restore_children([f], [id] if claimed else [], reason)
            raise RemovalError(reason, file_ids=[id])

        # the file is no longer assigned and its request resources are closed
        with self.lock:
            self.release_link(id)

            deleted = False
            try:
                self.pyload.db.delete_link(f)
                self.pyload.db.sync_save()
                deleted = self.pyload.db.get_link_data(id) is None
                if not deleted:
                    raise RuntimeError("link still present after deletion")
            except Exception as exc:
                # a follow-up error may still have removed the record: verify
                # before deciding whether the removal has to be rolled back
                try:
                    deleted = self.pyload.db.get_link_data(id) is None
                except Exception:
                    deleted = False

                if deleted:
                    self.pyload.log.warning(
                        self._(
                            "Link {id} was deleted but the operation reported: {msg}"
                        ).format(id=id, msg=exc)
                    )
                else:
                    reason = self._("Database error while deleting link")
                    self.pyload.log.error(
                        self._("Deleting link {id} failed: {msg}").format(
                            id=id, msg=exc
                        )
                    )
                    self._restore_children([f], [id], reason)
                    raise RemovalError(reason, file_ids=[id]) from exc

            # removal is durable: publish exactly once
            self.pyload.event_manager.add_event(RemoveEvent("file", id, destination))

            p = self.get_package(pid)
            package_removed = False
            if p is not None and not len(p.get_children()):
                # last link removed: clean the now empty package
                package_removed = p.delete()

            if not package_removed:
                pyfiles = self.cache.values()
                for pyfile in pyfiles:
                    if pyfile.packageid == pid and pyfile.order > oldorder:
                        pyfile.order -= 1
                        pyfile.notify_change()

            self.pyload.addon_manager.dispatch_event("file_deleted", id, pid)

        return True

    def _restore_children(self, pyfiles, stopped_ids, reason):
        """
        rollback a failed removal: bring every claimed pyfile back into the
        cache in a consistent, manageable state without persisting anything
        for deleted records. Files whose transfer failed to converge are put
        in the stopped (aborted), retryable state.
        """
        for pyfile in pyfiles:
            pyfile.unclaim_removal()
            self.cache[pyfile.id] = pyfile

            if pyfile.id in stopped_ids:
                pyfile.abort = True
                pyfile.status = 9  #: aborted: stopped and retryable
                pyfile.error = reason
            else:
                pyfile.abort = False

            try:
                # persist the in-memory state the (now finished) worker left,
                # keeping cache and database consistent after the rollback
                pyfile.sync()
            except Exception:
                pass

    # ----------------------------------------------------------------------
    def release_link(self, id):
        """
        removes pyfile from cache.
        """
        if id in self.cache:
            del self.cache[id]

    # ----------------------------------------------------------------------
    def release_package(self, id):
        """
        removes package from cache.
        """
        if id in self.package_cache:
            del self.package_cache[id]

    # ----------------------------------------------------------------------
    def update_link(self, pyfile):
        """
        updates link.
        """
        self.pyload.db.update_link(pyfile)

        e = UpdateEvent(
            "file", pyfile.id, "collector" if not pyfile.package().queue else "queue"
        )
        self.pyload.event_manager.add_event(e)

    # ----------------------------------------------------------------------
    def update_package(self, pypack):
        """
        updates a package.
        """
        self.pyload.db.update_package(pypack)

        e = UpdateEvent("pack", pypack.id, "collector" if not pypack.queue else "queue")
        self.pyload.event_manager.add_event(e)

    # ----------------------------------------------------------------------
    def get_package(self, id):
        """
        return package instance.
        """
        if id in self.package_cache:
            return self.package_cache[id]
        else:
            return self.pyload.db.get_package(id)

    # ----------------------------------------------------------------------
    def get_package_data(self, id):
        """
        returns dict with package information.
        """
        pack = self.get_package(id)

        if not pack:
            return None

        pack = pack.to_dict()[id]

        data = self.pyload.db.get_package_data(id)

        tmplist = []

        cache = list(self.cache.values())
        for x in cache:
            if int(x.to_db_dict()[x.id]["package"]) == int(id):
                tmplist.append((x.id, x.to_db_dict()[x.id]))
        data.update(tmplist)

        pack["links"] = data

        return pack

    # ----------------------------------------------------------------------
    def get_file_data(self, id):
        """
        returns dict with file information.
        """
        if id in self.cache:
            return self.cache[id].to_db_dict()

        return self.pyload.db.get_link_data(id)

    # ----------------------------------------------------------------------
    def get_file(self, id):
        """
        returns pyfile instance.
        """
        if id in self.cache:
            return self.cache[id]
        else:
            return self.pyload.db.get_file(id)

    # ----------------------------------------------------------------------
    @lock
    def get_job(self, occupied):
        """
        get suitable job.
        """
        # TODO: clean mess
        # TODO: improve selection of valid jobs

        if occupied in self.job_cache:
            if self.job_cache[occupied]:
                id = self.job_cache[occupied].pop()
                if id == "empty":
                    pyfile = None
                    self.job_cache[occupied].append("empty")
                else:
                    pyfile = self.get_file(id)
                    # Handle case where file was deleted but still in cache
                    if pyfile is None and self.job_cache[occupied]:
                        # Try next file in cache
                        return self.get_job(occupied)
            else:
                jobs = self.pyload.db.get_job(occupied)
                jobs.reverse()
                if not jobs:
                    self.job_cache[occupied].append("empty")
                    pyfile = None
                else:
                    self.job_cache[occupied].extend(jobs)
                    pyfile = self.get_file(self.job_cache[occupied].pop())
                    # Handle case where file was deleted
                    if pyfile is None and self.job_cache[occupied]:
                        return self.get_job(occupied)

        else:
            self.job_cache = {}  #: better not caching too much
            jobs = self.pyload.db.get_job(occupied)
            jobs.reverse()
            self.job_cache[occupied] = jobs

            if not jobs:
                self.job_cache[occupied].append("empty")
                pyfile = None
            else:
                pyfile = self.get_file(self.job_cache[occupied].pop())
                # Handle case where file was deleted
                if pyfile is None and self.job_cache[occupied]:
                    return self.get_job(occupied)

            # TODO: maybe the new job has to be approved...

        # pyfile = self.get_file(self.job_cache[occ].pop())
        return pyfile

    @lock
    def get_decrypt_job(self):
        """
        return job for decrypting.
        """
        if "decrypt" in self.job_cache and self.job_cache["decrypt"] == "empty":
            return None

        plugins = tuple(
            chain(
                self.pyload.plugin_manager.decrypter_plugins.keys(),
                self.pyload.plugin_manager.container_plugins.keys(),
            )
        )

        jobs = self.pyload.db.get_plugin_job(plugins)
        if jobs:
            return self.get_file(jobs[0])
        else:
            self.job_cache["decrypt"] = "empty"
            return None

    def get_file_count(self):
        """
        returns number of files.
        """
        if self.filecount == -1:
            self.filecount = self.pyload.db.filecount(1)

        return self.filecount

    def get_queue_count(self, force=False):
        """
        number of files that have to be processed.
        """
        if self.queuecount == -1 or force:
            self.queuecount = self.pyload.db.queuecount(1)

        return self.queuecount

    def check_all_links_finished(self):
        """
        checks if all files are finished and dispatch event.
        """
        if not self.get_queue_count(True):
            self.pyload.addon_manager.dispatch_event("all_downloads_finished")
            self.pyload.log.debug("All downloads finished")
            return True

        return False

    def check_all_links_processed(self, fid):
        """
        checks if all files was processed and pyload would idle now, needs fid which
        will be ignored when counting.
        """
        # reset count so statistic will update (this is called when dl was processed)
        self.reset_count()

        if not self.pyload.db.processcount(1, fid):
            self.pyload.addon_manager.dispatch_event("all_downloads_processed")
            self.pyload.log.debug("All downloads processed")
            return True

        return False

    def reset_count(self):
        self.queuecount = -1

    @lock
    @change
    def restart_package(self, id):
        """
        restart package.
        """
        pyfiles = list(self.cache.values())
        for pyfile in pyfiles:
            if pyfile.packageid == id:
                self.restart_file(pyfile.id)

        self.pyload.db.restart_package(id)

        if id in self.package_cache:
            self.package_cache[id].set_finished = False

        e = UpdateEvent(
            "pack", id, "collector" if not self.get_package(id).queue else "queue"
        )
        self.pyload.event_manager.add_event(e)

    @lock
    @change
    def restart_file(self, id):
        """
        restart file.
        """
        if id in self.cache:
            self.cache[id].status = 3
            self.cache[id].name = self.cache[id].url
            self.cache[id].error = ""
            self.cache[id].abort_download()

        self.pyload.db.restart_file(id)

        e = UpdateEvent(
            "file",
            id,
            "collector" if not self.get_file(id).package().queue else "queue",
        )
        self.pyload.event_manager.add_event(e)

    @lock
    @change
    def set_package_location(self, package_id, queue):
        """
        move package to queue / collector.
        """
        queue = queue.value
        p = self.pyload.db.get_package(package_id)
        if not p:
            return

        old_queue = p.queue
        oldorder = p.order

        e = RemoveEvent("pack", package_id, "collector" if not old_queue else "queue")
        self.pyload.event_manager.add_event(e)

        # Clear package order in old queue and adjust other packages
        self.pyload.db.clear_package_order(p)

        # Update package location
        p = self.pyload.db.get_package(package_id)
        p.queue = queue
        self.pyload.db.update_package(p)

        # Reorder package to end of new queue
        self.pyload.db.reorder_package(p, -1, True)

        # Update cached packages in old queue (database already updated by clear_package_order)
        packs = list(self.package_cache.values())
        for pack in packs:
            if pack.queue == old_queue and pack.order > oldorder and pack.id != package_id:
                pack.order -= 1
                pack.notify_change()

        self.pyload.db.commit()

        self.release_package(package_id)
        p = self.get_package(package_id)

        e = InsertEvent("pack", package_id, p.order, "collector" if not p.queue else "queue")
        self.pyload.event_manager.add_event(e)

    @lock
    @change
    def reorder_package(self, id, position):
        p = self.get_package(id)
        if not p:
            return

        old_position = p.order
        if old_position == position:
            return

        e = RemoveEvent("pack", id, "collector" if not p.queue else "queue")
        self.pyload.event_manager.add_event(e)
        self.pyload.db.reorder_package(p, position)

        packs = list(self.package_cache.values())
        for pack in packs:
            if pack.queue != p.queue or pack.order < 0 or pack.id == id:
                continue

            if old_position > position:
                if position <= pack.order < old_position:
                    pack.order += 1
                    pack.notify_change()
            elif old_position < position:
                if old_position < pack.order <= position:
                    pack.order -= 1
                    pack.notify_change()

        p.order = position
        self.pyload.db.commit()

        e = InsertEvent("pack", id, position, "collector" if not p.queue else "queue")
        self.pyload.event_manager.add_event(e)

    @lock
    @change
    def reorder_file(self, id, position):
        f = self.get_file_data(id)
        if not f:
            return
        f = f[id]

        p = self.get_package(f["package"])
        if not p:
            return

        old_position = f["order"]

        e = RemoveEvent(
            "file",
            id,
            "collector" if not p.queue else "queue",
        )
        self.pyload.event_manager.add_event(e)

        self.pyload.db.reorder_link(f, position)

        pyfiles = list(self.cache.values())
        for pyfile in pyfiles:
            if pyfile.packageid != f["package"] or pyfile.order < 0 or pyfile.id == id:
                continue
            if old_position > position:
                if pyfile.order >= position and pyfile.order < old_position:
                    pyfile.order += 1
                    pyfile.notify_change()
            elif old_position < position:
                if pyfile.order <= position and pyfile.order > old_position:
                    pyfile.order -= 1
                    pyfile.notify_change()

        if id in self.cache:
            self.cache[id].order = position

        self.pyload.db.commit()

        e = InsertEvent(
            "file",
            id,
            position,
            "collector" if not p.queue else "queue",
        )
        self.pyload.event_manager.add_event(e)

    @change
    def update_file_info(self, data, pid):
        """
        updates file info (name, size, status, url)
        """
        self.pyload.db.update_link_info(data)
        e = UpdateEvent(
            "pack", pid, "collector" if not self.get_package(pid).queue else "queue"
        )
        self.pyload.event_manager.add_event(e)

    def check_package_finished(self, pyfile):
        """
        checks if package is finished and calls addon_manager.
        """
        ids = self.pyload.db.get_unfinished(pyfile.packageid)
        if not ids or (pyfile.id in ids and len(ids) == 1):
            if not pyfile.package().set_finished:
                self.pyload.log.info(
                    self._("Package finished: {}").format(pyfile.package().name)
                )
                self.pyload.addon_manager.package_finished(pyfile.package())
                pyfile.package().set_finished = True

    def recheck_package(self, pid):
        """
        recheck links in package.
        """
        data = self.pyload.db.get_package_data(pid)

        urls = []

        for pyfile in data.values():
            if pyfile["status"] not in (0, 12, 13):
                urls.append((pyfile["url"], pyfile["plugin"]))

        self.pyload.thread_manager.create_info_thread(urls, pid)

    @lock
    @change
    def delete_finished_links(self):
        """
        deletes finished links and packages, return deleted packages.
        """
        old_packs = self.get_info_data(Destination.QUEUE)
        old_packs.update(self.get_info_data(Destination.COLLECTOR))

        self.pyload.db.delete_finished()

        new_packs = self.pyload.db.get_all_packages(0)
        new_packs.update(self.pyload.db.get_all_packages(1))
        # get new packages only from db

        deleted = []
        for id in old_packs.keys():
            if id not in new_packs:
                deleted.append(id)
                self.delete_package(int(id))

        return deleted

    @lock
    @change
    def restart_failed(self):
        """
        restart all failed links.
        """
        self.pyload.db.restart_failed()
