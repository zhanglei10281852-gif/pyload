from ..network.exceptions import Abort, Fail, Retry
from .plugin_thread import PluginThread


class DecrypterThread(PluginThread):
    """
    thread for decrypting.
    """

    def __init__(self, manager, pyfile):
        """
        constructor.
        """
        super().__init__(manager)

        self.active = pyfile
        manager.local_threads.append(self)

        pyfile.set_status("decrypting")

        self.start()

    def get_active_files(self):
        return [self.active]

    def run(self):
        """
        run method.
        """
        pyfile = self.active
        retry = False

        if pyfile.removed:
            # claimed for deletion before decryption even started
            self._finish()
            return

        try:
            self.pyload.log.info(
                self._("Decrypting starts: {}").format(self.active.name)
            )
            self.active.plugin.preprocessing(self)

        except NotImplementedError:
            if self.active.removed:
                return

            self.pyload.log.error(
                self._("Plugin {} is missing a function").format(self.active.pluginname)
            )
            return

        except Fail as exc:
            msg = exc.args[0]

            if self.active.removed:
                return

            if msg == "offline":
                self.active.set_status("offline")
                self.pyload.log.warning(
                    self._("Download is offline: {}").format(self.active.name)
                )
            else:
                self.active.set_status("failed")
                self.pyload.log.error(
                    self._("Decrypting failed: {name} | {msg}").format(
                        name=self.active.name, msg=msg
                    )
                )
                self.active.error = msg

            return

        except Abort:
            self.pyload.log.info(self._("Download aborted: {}").format(pyfile.name))
            pyfile.set_status("aborted")

            return

        except Retry:
            if self.active.removed or self.active.abort:
                # a late retry must not resurrect a cancelled/deleted job
                self.pyload.log.info(
                    self._("Download aborted: {}").format(self.active.name)
                )
                self.active.set_status("aborted")
                return

            self.pyload.log.info(self._("Retrying {}").format(self.active.name))
            retry = True
            return self.run()

        except Exception as exc:
            if self.active.removed:
                return

            self.active.set_status("failed")
            self.pyload.log.warning(
                self._("Decrypting failed: {name} | {msg}").format(
                    name=self.active.name, msg=exc
                ),
                exc_info=self.pyload.debug > 1,
                stack_info=self.pyload.debug > 2,
            )
            self.active.error = str(exc)

            if self.pyload.debug:
                self.write_debug_report(pyfile)

            return

        finally:
            if not retry:
                self._finish()

        # self.pyload.addon_manager.download_finished(pyfile)

        # self.m.local_threads.remove(self)
        # self.active.finish_if_done()
        if not retry and not pyfile.removed:
            pyfile.delete()

    def _finish(self):
        """
        worker epilogue: release resources and leave the active set, without
        writing removed records back.
        """
        try:
            self.active.release()
        finally:
            self.pyload.files.save()
            try:
                self.m.local_threads.remove(self)
            except ValueError:
                pass
            # leave the active set only after the thread is unregistered,
            # so processing_ids() never exposes a bare ``False`` entry
            self.active = False
            # exc_clear()
