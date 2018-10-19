class BaseDumpDownloadHelper:

    def import_from_dump(self):
        """Given the downloaded dump path, 
        this method is called to perform the import into the database

        Raises:
            NotImplementedError -- You have to override this method
        """

        raise NotImplementedError

    def dump_download_uri(self) -> str:
        """Implement this func to return a computed dump uri.
        Useful if there is a way to compute the latest dump uri.

        Raises:
            NotImplementedError -- You can avoid the override, it's not an issue
        """

        raise NotImplementedError
