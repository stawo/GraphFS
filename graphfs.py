#!/usr/bin/env python3

import os
import sys
import errno
from errno import ENOENT
import stat  # S_IFDIR, S_IFLNK, S_IFREG
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

    def __isGroup(self, groupId):
        if not Group.select(self.graph, groupId).first() is None:
            return True

        return False

    def __isFile(self, fileId):
        if not File.select(self.graph, fileId).first() is None:
            return True

        return False
    
    def __verifyPath(self, path, lastElementMustExist = False):
        """
        A path is valid if all the elements apart the last one are existing groups.
        If lastElementMustExist is True, then the last element must be either a Group or a File
        """
        elementsIDs = self.__parsePathInGroups(path)

        if elementsIDs is None:
            return True
        elif lastElementMustExist:
            if len(elementsIDs) > 1:
                if all(self.__isGroup(element) for element in elementsIDs[:-1]) \
                    and (self.__isGroup(elementsIDs[-1]) \
                        or self.__isFile(elementsIDs[-1])):
                    return True
                else:
                    return False
            else:
                if self.__isGroup(elementsIDs[-1]) \
                    or self.__isFile(elementsIDs[-1]):
                    return True
                else:
                    return False
        else:    
            if len(elementsIDs) > 1:
                if all(self.__isGroup(element) for element in elementsIDs[:-1]):
                    return True
                else:
                    return False
            else:
                return True
        
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

        
        # Check if the last element refers to a group or a file
        if path == "/":
            return dict(
                st_mode=(stat.S_IFDIR | 0o755)
                , st_nlink=1
                , st_size=1024
                , st_ctime=time()
                , st_mtime=time()
                , st_atime=time()
                , st_uid = os.getuid()
                , st_gid = os.getgid()
            )

        if not self.__verifyPath(path, lastElementMustExist=True):
            raise FuseOSError(errno.ENOENT)
            
        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)
        
        if self.__isGroup(groupIDs[-1]):
            return dict(
                st_mode=(stat.S_IFDIR | 0o755)
                , st_nlink=2
                , st_size=1024
                , st_ctime=time()
                , st_mtime=time()
                , st_atime=time()
                , st_uid = os.getuid()
                , st_gid = os.getgid()
            )
        
        if self.__isFile(groupIDs[-1]):
            return dict(
                st_mode=(stat.S_IFREG | 0o755)
                , st_nlink=1
                , st_size=1024
                , st_ctime=time()
                , st_mtime=time()
                , st_atime=time()
                , st_uid = os.getuid()
                , st_gid = os.getgid()
            )
        
        # The path does not refer to a group nor a file
        raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh=None):
        
        # How to manage the fact that a file can have the same name as a group?

        print("-------")
        print("readdir: {}".format(path))

        dirents = ['.', '..']
        
        if not self.__verifyPath(path, lastElementMustExist=True):
            raise OSError("The path [{}] is invalid".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        # Retrieve all the groups
        if groupIDs is None:
            query = "MATCH (g:Group) RETURN g.name as name"
        else:
            # Retrieve files that are connected to all the groups
            # We achieve that by checking which files belong to the groups specified in the path
            # and see if they belong to additional groups as well
            query = """WITH {groupIDs} as groups
                MATCH (g:Group)<-[:isInGroup]-(f:File)-[:isInGroup]->(gNew:Group)
                WHERE g.name in groups
                and not gNew.name in groups
                WITH gNew, size(groups) as inputCnt, count(DISTINCT g) as cnt
                WHERE cnt = inputCnt
                RETURN gNew.name as name""".format(groupIDs = list(groupIDs))
        
        print("query: {}".format(query))
        
        queryResults = self.graph.run(query)
        
        dirents.extend([queryResult["name"] for queryResult in queryResults])
        
        # Retrieve all the files
        if groupIDs is None:
            query = "MATCH (f:File) RETURN f.name as name"
        else:
            # Retrieve files that are connected to all the groups
            # We achieve that by checking which files belong to the groups specified in the path
            query = """WITH {groupIDs} as groups
                MATCH (g:Group)<-[:isInGroup]-(f:File)
                WHERE g.name in groups
                WITH f, size(groups) as inputCnt, count(DISTINCT g) as cnt
                WHERE cnt = inputCnt
                RETURN f.name as name""".format(groupIDs = list(groupIDs))
        
        print("query: {}".format(query))
        
        queryResults = self.graph.run(query)
        
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
        
        if not self.__verifyPath(path, lastElementMustExist=True):
            raise OSError("The path [{}] is invalid".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Cannot remove root.")
        
        # Check if the last element exists already as a group
        if not self.__isGroup(groupIDs[-1]):
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

        if not self.__verifyPath(path):
            raise OSError("The path [{}] is invalid".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which could be a file.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Cannot create root.")
        
        # Check if the last element exists already as a group
        if self.__isGroup(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The group {} already exists".format(groupIDs[-1]))
        
        # Check if the last element exists already as a file
        if self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} already exists".format(groupIDs[-1]))
        
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
        """
        Rename the file, directory, or other object "from" to the target "to".
        Note that the source and target don't have to be in the same directory,
        so it may be necessary to move the source to an entirely new directory.
        """
        print("-------")
        print("rename")
        print("old: " + old)
        print("new: " + new)

        if not isinstance(old, str):
            raise TypeError("The element to rename must be a string.")
        if not isinstance(new, str):
            raise TypeError("The new name must be a string.")
        
        if not self.__verifyPath(old, lastElementMustExist=True):
            raise OSError("The path [{}] is invalid".format(old))
        if not self.__verifyPath(new):
            raise OSError("The path [{}] is invalid".format(new))

        
        oldGroupIDs = self.__parsePathInGroups(old)
        newGroupIDs = self.__parsePathInGroups(new)    
        
        if oldGroupIDs is None:
            raise OSError("Cannot rename/move root.")
        
        if newGroupIDs is None:
            # All groups and files are already in the root group,
            # so we do not have to do anything.
            return
            
        # Check if the last element newGroupIDs 
        # is the same of oldGroupIDs. In yes, it means we
        # are trying to move the old element
        if oldGroupIDs[-1] == newGroupIDs[-1]:
            
            # Remove the last element, the remaining ones are for sure valid groups
            newGroupIDs = newGroupIDs[:-1] 

            # We have to move old into new
            
            # Check if last element of old is a group
            if self.__isGroup(oldGroupIDs[-1]):
                raise OSError("Cannot move folder into a folder.")
            
            elif self.__isFile(oldGroupIDs[-1]):
                # We have to:
                # - remove file oldGroupIDs[-1] from all the groups in oldGroupIDs[:-1]
                # - add file oldGroupIDs[-1] to all the groups newGroupIDs
                
                for groupId in oldGroupIDs[:-1]:
                    query = "MATCH (f:File)-[r:isInGroup]->(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' DELETE r".format(
                        fileId = oldGroupIDs[-1]
                        , groupId = groupId
                        )
                    
                    queryResults = self.graph.run(query)
                
                for groupId in newGroupIDs:
                    query = "MATCH (f:File),(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' CREATE (f)-[r:isInGroup]->(g) RETURN r".format(
                        fileId = oldGroupIDs[-1]
                        , groupId = groupId
                        )
                    
                    queryResults = self.graph.run(query)
            
            else:
                # We shouldn't be here
                raise Exception('Something went wrong.')
            
        else:
            
            # Check if last element of old is a group
            if self.__isGroup(oldGroupIDs[-1]):

                if self.__isFile(newGroupIDs[-1]):
                    raise FileExistsError("The file {} already exists".format(newGroupIDs[-1]))

                elif self.__isGroup(newGroupIDs[-1]):
                    # We have to move group oldGroupIDs[-1] into all the groups newGroupIDs
                    raise OSError("Cannot move folder into a folder.")                    
                    
                else:
                    # We have to:
                    # - rename group oldGroupIDs[-1] into newGroupIDs[-1],
                    # - move group oldGroupIDs[-1] into all the groups newGroupIDs[:-1]
                    query = """MATCH (g:Group {{ name: '{oldGroupId}' }})
                        SET g.name = '{newGroupId}'
                        RETURN g""".format(
                            oldGroupId = oldGroupIDs[-1]
                            , newGroupId = newGroupIDs[-1])

                    queryResults = self.graph.run(query)
            
            elif self.__isFile(oldGroupIDs[-1]):
                
                if self.__isFile(newGroupIDs[-1]):
                    raise FileExistsError("The file {} already exists".format(newGroupIDs[-1]))

                elif self.__isGroup(newGroupIDs[-1]):
                    # We have to:
                    # - remove file oldGroupIDs[-1] from all the groups in oldGroupIDs[:-1]
                    # - add file oldGroupIDs[-1] to all the groups newGroupIDs
                    
                    for groupId in oldGroupIDs[:-1]:
                        query = "MATCH (f:File)-[r:isInGroup]->(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' DELETE r".format(
                            fileId = oldGroupIDs[-1]
                            , groupId = groupId
                            )
                        
                        queryResults = self.graph.run(query)
                    
                    for groupId in newGroupIDs:
                        query = "MATCH (f:File),(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' CREATE (f)-[r:isInGroup]->(g) RETURN r".format(
                            fileId = oldGroupIDs[-1]
                            , groupId = groupId
                            )
                        
                        queryResults = self.graph.run(query)

                else:
                    # We have to:
                    # - rename file oldGroupIDs[-1] into newGroupIDs[-1],
                    # - remove file oldGroupIDs[-1] from all the groups in oldGroupIDs[:-1]
                    # - add file oldGroupIDs[-1] to all the groups newGroupIDs
                    query = """MATCH (f:File {{ name: '{oldFileId}' }})
                        SET f.name = '{newFileId}'
                        RETURN f""".format(
                            oldFileId = oldGroupIDs[-1]
                            , newFileId = newGroupIDs[-1])

                    queryResults = self.graph.run(query)

                    # We remove file oldGroupIDs[-1] from groups oldGroupIDs[:-1]
                    for groupId in oldGroupIDs[:-1]:
                        query = "MATCH (f:File)-[r:isInGroup]->(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' DELETE r".format(
                            fileId = oldGroupIDs[-1]
                            , groupId = groupId
                            )
                        
                        queryResults = self.graph.run(query)
                    
                    # We have to move file old into folders new
                    for groupId in newGroupIDs:
                        query = "MATCH (f:File),(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' CREATE (f)-[r:isInGroup]->(g) RETURN r".format(
                            fileId = oldGroupIDs[-1]
                            , groupId = groupId
                            )
                        
                        queryResults = self.graph.run(query)
            else:
                # We shouldn't be here
                raise Exception('Something went wrong.')
        

    def link(self, target, name):
        # return os.link(self._full_path(name), self._full_path(target))
        pass
        
    def utimens(self, path, times=None):
        # return os.utime(self._full_path(path), times)
        pass
        
    # File methods
    # ============

    def open(self, path, flags):
        
        print("-------")
        print("open {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which is the file name.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Must specify a proper file name.")
        
        # Check if the last element exists already as a file
        if self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} already exists".format(groupIDs[-1]))
        
        return True

    def create(self, path, mode, fi=None):
        
        print("-------")
        print("create {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which is the file name.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Must specify a proper file name.")
        
        # Check if the last element exists already as a file
        if self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} already exists".format(groupIDs[-1]))
        
        # Check if the last element exists already as a group
        if self.__isGroup(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The group {} already exists".format(groupIDs[-1]))
        
        print("Create file {}".format(groupIDs[-1]))

        query = "CREATE (f:File{{name:'{fileId}'}})".format(fileId = groupIDs[-1])
        queryResults = self.graph.run(query)
        
        # Link the file to all the groups appearing in groupIDs
        if len(groupIDs) > 1:
            for groupId in groupIDs[:-1]:
                query = "MATCH (f:File),(g:Group) WHERE f.name = '{fileId}' AND g.name = '{groupId}' CREATE (f)-[r:isInGroup]->(g) RETURN r".format(
                    fileId = groupIDs[-1]
                    , groupId = groupId
                    )
                
                queryResults = self.graph.run(query)

        return 0

    def read(self, path, length, offset, fh):
        
        print("-------")
        print("read {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which is the file name.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Must specify a proper file name.")
        
        # Check if the last element exists already as a file
        if not self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} does not exists".format(groupIDs[-1]))
        
        query = "MATCH (f:File{{name:'{fileId}'}}) RETURN f.value".format(fileId = groupIDs[-1])
        queryResults = self.graph.run(query)
        
        return queryResults

    def write(self, path, buf, offset, fh):
        
        print("-------")
        print("write {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which is the file name.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Must specify a proper file name.")
        
        # Check if the last element exists already as a file
        if not self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} does not exists".format(groupIDs[-1]))
        
        query = "MATCH (f:File{{name:'{fileId}'}}) SET f.value = '{value}' RETURN f".format(
            fileId = groupIDs[-1]
            , value = buf
            )
        queryResults = self.graph.run(query)
        
        return len(buf)

    def truncate(self, path, length, fh=None):
        
        print("-------")
        print("write {}".format(path))

        # Split the path in single elements.
        # Each element is a group, apart the last one, which is the file name.
        # First we normalize the path (so we have all '/' as delimiters),
        # then we remove the eventual drive letter, as we don't need it.
        groupIDs = self.__parsePathInGroups(path)

        if groupIDs is None:
            raise OSError("Must specify a proper file name.")
        
        # Check if the last element exists already as a file
        if not self.__isFile(groupIDs[-1]):
            # It already exists
            raise FileExistsError("The file {} does not exists".format(groupIDs[-1]))
        
        query = "MATCH (f:File{{name:'{fileId}'}}) SET f.value = NULL RETURN f".format(
            fileId = groupIDs[-1]
            )
        queryResults = self.graph.run(query)

    def flush(self, path, fh):
        # return os.fsync(fh)
        pass

    def release(self, path, fh):
        # return os.close(fh)
        pass

    def fsync(self, path, fdatasync, fh):
        # return self.flush(path, fh)
        pass


if __name__ == '__main__':
    
    FUSE(GraphFSNeo4j(), 'Prova', nothreads=True, foreground=True, debug=True)
    