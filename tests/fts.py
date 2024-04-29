import dmm.utils.fts as fts

class A:
    def __init__(self):
        self.src_url = "xrootd-sense-ucsd-redirector.sdsc.optiputer.net:1094"
        self.dst_url = "sense-redir-01.ultralight.org:1094"

test = A()

print(fts.modify_link_config(test, 1000, 1000))
print(fts.modify_se_config(test, 1000, 1000))