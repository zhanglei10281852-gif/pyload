from enum import Enum
from typing import Optional, Any

from pydantic import BaseModel, Field

from pyload.core.datatypes.enums import DownloadStatus
from pyload.core.datatypes.json_schema_extras import FLOAT_JSON_SCHEMA
from pyload.core.datatypes.json_schema_extras import INT64_JSON_SCHEMA
from pyload.core.datatypes.json_schema_extras import OPTIONAL_FLOAT_JSON_SCHEMA
from pyload.core.datatypes.json_schema_extras import OPTIONAL_INT64_JSON_SCHEMA


class OnlineCheckStatus(str, Enum):
    #: the check is still producing result batches
    RUNNING = "running"
    #: every result batch has been produced and the check finished cleanly
    COMPLETE = "complete"
    #: a hoster plugin raised while fetching online status information
    PLUGIN_ERROR = "plugin_error"
    #: a submitted container file could not be decrypted
    CONTAINER_ERROR = "container_error"
    #: the check expired before all results were produced
    EXPIRED = "expired"
    #: the result id is unknown (never existed, garbage collected or pre-restart)
    UNKNOWN = "unknown"


class AccountInfo(BaseModel):
    validuntil: Optional[float] = Field(default=None, json_schema_extra=OPTIONAL_FLOAT_JSON_SCHEMA)
    login: str = ""
    options: dict = {}
    valid: bool = False
    trafficleft: Optional[int] = Field(default=None, json_schema_extra=OPTIONAL_INT64_JSON_SCHEMA)
    premium: bool = False
    type: str = ""


class CaptchaTask(BaseModel):
    tid: int
    data: Optional[dict] = None
    type: Optional[str] = None
    result_type: Optional[str] = None


class ConfigItem(BaseModel):
    name: str
    description: str
    value: str
    type: str


class ConfigSection(BaseModel):
    name: str
    description: str
    items: list[ConfigItem]
    outline: Optional[str]


class DownloadInfo(BaseModel):
    fid: int
    name: str
    speed: int = Field(json_schema_extra=INT64_JSON_SCHEMA)
    eta: int
    format_eta: str
    bleft: int = Field(json_schema_extra=INT64_JSON_SCHEMA)
    size: int = Field(json_schema_extra=INT64_JSON_SCHEMA)
    format_size: str
    percent: int
    status: DownloadStatus
    statusmsg: str
    format_wait: str
    wait_until: float = Field(json_schema_extra=FLOAT_JSON_SCHEMA)
    package_id: int
    package_name: str
    plugin: str
    info: str # NOTE: needed by webui, remove in future...


class EventInfo(BaseModel):
    eventname: str
    id: Optional[str] = None
    type: Optional[int] = None
    destination: Optional[int] = None


class FileData(BaseModel):
    fid: int
    url: str
    name: str
    plugin: str
    size: int = Field(json_schema_extra=INT64_JSON_SCHEMA)
    format_size: str
    status: DownloadStatus
    statusmsg: str
    package_id: int
    error: str
    order: int


class OnlineCheck(BaseModel):
    rid: int
    data: dict = {}
    #: batch cursor this reply starts at (echo of the poll request); absent on legacy replies
    progress: Optional[int] = None
    #: batch cursor to send with the next poll request
    next_progress: Optional[int] = None
    #: lifecycle state for cursor based polling, see `OnlineCheckStatus`
    status: Optional[str] = None
    #: distinct terminal failure kinds when the check did not finish cleanly
    errors: Optional[list[str]] = None

    def __iter__(self):
        # Optional fields that were not populated are omitted from dict()/JSON
        # output, so replies on the legacy single-consumer path stay identical
        # to the previous {"rid": ..., "data": ...} wire format.
        return iter(
            (key, value)
            for key, value in super().__iter__()
            if value is not None
        )


class OnlineStatus(BaseModel):
    name: str
    plugin: str
    packagename: str
    status: int
    size: int = Field(json_schema_extra=INT64_JSON_SCHEMA)


class PackageData(BaseModel):
    pid: int
    name: str
    folder: str
    site: str
    password: str
    dest: int
    order: int
    linksdone: Optional[int] = None
    sizedone: Optional[int] = Field(default=None, json_schema_extra=OPTIONAL_INT64_JSON_SCHEMA)
    sizetotal: Optional[int] = Field(default=None, json_schema_extra=OPTIONAL_INT64_JSON_SCHEMA)
    linkstotal: Optional[int] = None
    links: Optional[list[FileData]] = None
    fids: Optional[list[int]] = None


class ServerStatus(BaseModel):
    pause: bool
    active: int
    queue: int
    total: int
    speed: int = Field(json_schema_extra=INT64_JSON_SCHEMA)
    download: bool
    reconnect: bool
    captcha: bool
    proxy: bool


class ServiceCall(BaseModel):
    plugin: str
    func: str
    arguments: Optional[list[Any]]
    parse_arguments: bool


#: Needed by legacy API
class OldUserData(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[int] = None
    permission: Optional[int] = None
    template_name: Optional[str] = None


class UserData(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[int] = None
    permission: Optional[int] = None
    template: Optional[str] = None
