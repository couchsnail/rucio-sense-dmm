import dmm.utils.fts as fts

class A:
    def __init__(self):
        self.dst_url = "xrootd-sense-ucsd-redirector-111.sdsc.optiputer.net:1094"
        self.src_url = "sense-redir-01.ultralight.org:1094"

test = A()

print(fts.modify_link_config(test, 400, 400))
print(fts.modify_se_config(test, 400, 400))