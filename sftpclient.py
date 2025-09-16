#!/usr/bin/env python3
"""
Wrapper class for SFTP connections
"""

from paramiko import ssh_exception, AutoAddPolicy, SSHClient


class SFTPClient():
    """Wrapper around third-party SFTP client"""

    def __init__(self, host, username, password, port: int = 22):
        """Set up SFTP connection"""
        paramiko = SSHClient()
        paramiko.set_missing_host_key_policy(AutoAddPolicy)
        try:
            paramiko.connect(hostname=host, username=username, password=password, port=port)
        except ssh_exception.AuthenticationException as error:
            raise SFTPAuthError(error)
        self.sftp = paramiko.open_sftp()

    def cwd(self, path):
        """Change working directory"""
        self.sftp.chdir(path=path)

    def mkdir(self, path):
        """Make new directory"""
        self.sftp.mkdir(path=path)

    def remove(self, path):
        """Remove specified file"""
        self.sftp.remove(path=path)

    def putfo(self, flo, remotepath):
        """Copy file-like object to server"""
        self.sftp.putfo(fl=flo, remotepath=remotepath)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Safely close SFTP connection"""
        self.sftp.close()


class SFTPAuthError(Exception):
    """Exception to report SFTP authentication errors"""
    def __init__(self, message):
        super().__init__(message)
