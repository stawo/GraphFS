#!/usr/bin/env python3

import os
import sys
import errno
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations

from pyArango.connection import *
from py2neo import Graph,Node, Relationship
from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom

# ---------------------------------------------------------
# Internal libraries

from lib.passthrough import Passthrough


class Group(GraphObject):
    __primarykey__ = "name"

    name = Property()

    hasFiles = RelatedFrom("File", "isInGroup")

class File(GraphObject):
    __primarykey__ = "name"

    name = Property()

    isInGroup = RelatedTo("Group", "isInGroup")
    
# ---------------------------------------------------------
# Main class

class GraphFSNeo4j(Passthrough):
    
    def __init__(self):
        
        self.graph = Graph(password="JAt2Y4pG$YvaIpVP")
        
        
    # Helpers
    # =======

    def __parsePathInGroups(self, path):
        
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = (os.path.normpath(os.path.splitdrive(path)[1]))
        if groupIDs == "/":
            return None
        else:
            if not "/" in groupIDs:
                return [groupIDs]
            else:
                groupIDs = groupIDs.split('/')
        
                if groupIDs[0] == "" and len(groupIDs) > 1:
                    return groupIDs[1:]
                else:
                    raise ValueError("There was an error parsing path [{}].".format(path))

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        # full_path = self._full_path(path)
        # if not os.access(full_path, mode):
        #     raise FuseOSError(errno.EACCES)
        # return True
        pass

    def chmod(self, path, mode):
        # full_path = self._full_path(path)
        # return os.chmod(full_path, mode)
        pass
        
    def chown(self, path, uid, gid):
        # full_path = self._full_path(path)
        # return os.chown(full_path, uid, gid)
        pass

    def getattr(self, path, fh=None):
        # full_path = self._full_path(path)
        # st = os.lstat(full_path)
        # return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
        #              'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        
        print("-------")
        print("getattr: {}".format(path))

        
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)
        if groupIDs is None:
            return dict(
                st_mode=(S_IFDIR | 0o755)
                , st_nlink=2
                , st_size=1024
                , st_ctime=time()
                , st_mtime=time()
                , st_atime=time()
                , st_uid = os.getuid()
                , st_gid = os.getgid()
            )
        
        # Check if the last element refers to a group or a file
        if Group.select(self.graph, groupIDs[-1]).first() is None and \
            File.select(self.graph, groupIDs[-1]).first() is None:
            raise FuseOSError(errno.ENOENT)

        return dict(
            st_mode=(S_IFDIR | 0o755)
            , st_nlink=1
            , st_size=1024
            , st_ctime=time()
            , st_mtime=time()
            , st_atime=time()
            , st_uid = os.getuid()
            , st_gid = os.getgid()
        )
        
    def readdir(self, path, fh=None):
        
        # How to manage the fact that a file can have the same name as a group?

        print("-------")
        print("readdir: {}".format(path))

        dirents = ['.', '..']
        
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            query = "MATCH (g:Group) RETURN g.name as name"
        else:
            # Retrieve files that are connected to all the groups
            # We achieve that by checking which files belong to the groups specified in the path
            # and see if they belong to additional groups as well
            # If groupIDs is empty, then we return all possible groups
            query = """WITH {groupIDs} as groups
                MATCH (g:Group)<-[:isInGroup]-(f:File)-[:isInGroup]->(gNew:Group)
                WHERE g.name in groups
                and not gNew.name in groups
                WITH gNew, size(groups) as inputCnt, count(DISTINCT g) as cnt
                WHERE cnt = inputCnt
                RETURN gNew.name as name""".format(groupIDs = list(groupIDs))
        
        print("query: {}".format(query))
        
        queryResults = self.graph.run(query)
        
        # for queryResult in queryResults:
        #     print (queryResult)

        # dirents.extend([os.path.join(*groupIDs, queryResult["name"]) for queryResult in queryResults])
        dirents.extend([queryResult["name"] for queryResult in queryResults])
        
        print("dirents: {}".format(dirents))

        for r in dirents:
            yield r

    def readlink(self, path):
        # pathname = os.readlink(self._full_path(path))
        # if pathname.startswith("/"):
        #     # Path name is absolute, sanitize it.
        #     return os.path.relpath(pathname, self.rootDB)
        # else:
        #     return pathname
        pass

    def mknod(self, path, mode, dev):
        # return os.mknod(self._full_path(path), mode, dev)
        pass

    def rmdir(self, path):
        
        print("-------")
        print("rmdir {}".format(path))
        
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Cannot remove root.")
        
        # Check if the last element exists already as a group
        if Group.select(self.graph, groupIDs[-1]).first() is None:
            # It already exists
            raise OSError("The group {} does not exists".format(groupIDs[-1]))
        
        # Check if the group contains files
        if len(Group.select(self.graph, groupIDs[-1]).first().hasFiles) > 0:
            # It already exists
            raise OSError("The group {} contains files".format(groupIDs[-1]))
        
        print("Delete group {}".format(groupIDs[-1]))

        query = "MATCH (g:Group{{name:'{groupId}'}}) DELETE g".format(groupId = groupIDs[-1])
        queryResults = self.graph.run(query)

    def mkdir(self, path, mode):
        
        print("-------")
        print("mkdir {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Cannot create root.")
        
        # Check if the last element exists already as a group
        if not Group.select(self.graph, groupIDs[-1]).first() is None:
            # It already exists
            raise FileExistsError("The group {} already exists".format(groupIDs[-1]))
        
        print("Create group {}".format(groupIDs[-1]))

        query = "CREATE (g:Group{{name:'{groupId}'}})".format(groupId = groupIDs[-1])
        queryResults = self.graph.run(query)

    def statfs(self, path):
        # full_path = self._full_path(path)
        # stv = os.statvfs(full_path)
        # return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
        #     'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
        #     'f_frsize', 'f_namemax'))
        return dict(f_bsize=1024, f_blocks=1, f_bavail=sys.maxint)
        
    def unlink(self, path):
        # return os.unlink(self._full_path(path))
        pass
        
    def symlink(self, name, target):
        # return os.symlink(target, self._full_path(name))
        pass
        
    def rename(self, old, new):
        
        print("-------")
        print("rename")
        print("old: " + old)
        print("new: " + new)

        if not isinstance(old, str):
            raise TypeError("The element to rename must be a string.")
        if not isinstance(new, str):
            raise TypeError("The new name must be a string.")
        
        oldGroupIDs = self.__parsePathInGroups(old)
        
        if groupIDs is None:
            raise OSError("Cannot rename root.")
        
        # We consider only the last element of oldGroupIDs, as it is the one we want
        # to rename
        old = oldGroupIDs[-1]

        newGroupIDs = self.__parsePathInGroups(new)    
        if newGroupIDs is None:
            raise OSError("Cannot rename a folder as root.")
        
        # We consider only the last element of newGroupIDs, as it is the one we want
        # to use as a name
        new = newGroupIDs[-1]

        # Check if old exists as a group
        if Group.select(self.graph, old).first() is None:
            # It already exists
            raise FileExistsError("The group {} doesn't exists".format(new))

        # Check if new exists already as a group
        if not Group.select(self.graph, new).first() is None:
            # It already exists
            raise FileExistsError("The group {} already exists".format(new))

        query = """MATCH (g:Group {{ name: '{groupId}' }})
        SET g.name = '{newGroupId}'
        RETURN g""".format(groupId = old, newGroupId = new)
        
        queryResults = self.graph.run(query)

    def link(self, target, name):
        # return os.link(self._full_path(name), self._full_path(target))
        pass
        
    def utimens(self, path, times=None):
        # return os.utime(self._full_path(path), times)
        pass
        
    # File methods
    # ============

    def open(self, path, flags):
        # full_path = self._full_path(path)
        # return os.open(full_path, flags)
        raise Exception("Operation not supported")
        
    def create(self, path, mode, fi=None):
        # full_path = self._full_path(path)
        # return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        raise Exception("Operation not supported")

    def read(self, path, length, offset, fh):
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.read(fh, length)
        raise Exception("Operation not supported")

    def write(self, path, buf, offset, fh):
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.write(fh, buf)
        raise Exception("Operation not supported")

    def truncate(self, path, length, fh=None):
        # full_path = self._full_path(path)
        # with open(full_path, 'r+') as f:
        #     f.truncate(length)
        raise Exception("Operation not supported")

    def flush(self, path, fh):
        # return os.fsync(fh)
        raise Exception("Operation not supported")

    def release(self, path, fh):
        # return os.close(fh)
        raise Exception("Operation not supported")

    def fsync(self, path, fdatasync, fh):
        # return self.flush(path, fh)
        raise Exception("Operation not supported")


if __name__ == '__main__':
    
    FUSE(GraphFSNeo4j(), 'Prova', nothreads=True, foreground=True, debug=True)
    