import dmm.utils.fts as fts

class A:
    def __init__(self):
        self.src_url = "davs://xrootd-sense-ucsd-redirector.sdsc.optiputer.net"
        self.dst_url = "davs://sense-redir-01.ultralight.org"

test = A()

print(fts.modify_link_config(test, 2000, 2000))
print(fts.modify_se_config(test, 2000, 2000))