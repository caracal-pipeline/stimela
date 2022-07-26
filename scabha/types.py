import os.path

# # marker for unset defaults
# class UnsetClass(object):
#     pass

UNSET = "<UNSET>"

class File(str):

    @property
    def NAME(self):
        return File(os.path.basename(self))

    @property
    def DIR(self):
        return File(os.path.dirname(self))

    @property
    def BASEPATH(self):
        return File(os.path.splitext(self)[0])

    @property
    def BASENAME(self):
        return File(os.path.splitext(self.NAME)[0])

    @property
    def EXT(self):
        return os.path.splitext(self)[1]


class Directory(File):
    pass

class MS(Directory):
    pass



