DATE_COLUMN = "data"
HOUR_COLUMN = "ora"
SIRAV_COLUMN = "sirav"


class EsedraFilenameError(Exception):
    filename: str
    reason: str

    def __init__(self, filename: str, reason: str) -> None:
        self.filename = filename
        self.reason = reason
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        return f"invalid filename '{self.filename}': {self.reason}"


class FilenamePrefixError(EsedraFilenameError):
    def __init__(self, filename: str) -> None:
        super().__init__(filename, "must start with one of 'H', 'D' or 'M'")


class SiravCodeError(EsedraFilenameError):
    def __init__(self, filename: str) -> None:
        super().__init__(filename, "cannot extract SIRAV code from it")
