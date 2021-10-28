import sys
sys.path.insert(0, "/data/tzhu/BidSpider")

from hebeieb import HebeiebBidCallSpider


if __name__ == "__main__":
    config_filepath = "examples/hebeieb/config.yaml"
    spider = HebeiebBidCallSpider(config_filepath)
    spider.start()
