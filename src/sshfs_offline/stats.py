import copy


class Stats:
    def __init__(self):
        self.init = 0
        self.destroy = 0
        self.create = 0
        self.listxattr = 0
        self.getattr = 0
        self.readdir = 0
        self.read = 0
        self.write = 0
        self.flush = 0
        self.unlink = 0
        self.mkdir = 0
        self.rmdir = 0
        self.rename = 0
        self.symlink = 0
        self.truncate = 0
        self.chmod = 0
        self.chown = 0
        self.utimens = 0
        self.readlink = 0
        self.statFs = 0
        self.exceptions = 0
        
    def capture(self, statsSnapshot: "Stats") -> dict[str, int]:
        stats = copy.deepcopy(self.__dict__)
        for key, value in stats.items():           
            if key in statsSnapshot.__dict__:
                stats[key] = value - statsSnapshot.__dict__[key]
            else:
                stats[key] = value
        statsSnapshot.__dict__ = copy.deepcopy(self.__dict__)
        return stats    

calledStatsSnapshot = Stats()
calledStats = Stats()

remoteStatsSnapshot = Stats()
remoteStats = Stats()